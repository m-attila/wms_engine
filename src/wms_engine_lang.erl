%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%% Workflow language interpreter.
%%% @end
%%% Created : 03. May 2019 19:14
%%%-------------------------------------------------------------------
-module(wms_engine_lang).
-author("Attila Makra").
-include("wms_engine_lang.hrl").
-include_lib("wms_logger/include/wms_logger.hrl").

-ifdef(TEST).
-compile([export_all]).
-endif.

%% API
-export([execute/2]).

%% =============================================================================
%% Types
%% =============================================================================
-type tree_state() :: undefined | boolean().

%% =============================================================================
%% Callbacks
%% =============================================================================
-include_lib("wms_state/include/wms_state_variable_callbacks.hrl").

% variable handling behavior will be completed with command executions functions.

-callback execute_interaction(State :: engine_state(),
                              InteractionID :: identifier_name(),
                              ParameterValues :: [parameter_value()]) ->
                               {ok, return_values(), engine_state()}.
-callback wait_events(State :: engine_state(),
                      wait_type(),
                      EventIDS :: [identifier_name()]) ->
                       {ok, engine_state()}.
-callback fire_event(State :: engine_state(),
                     EventID :: identifier_name()) ->
                      {ok, engine_state()}.

-callback log_task_status(State :: engine_state(),
                          Type ::
                          started | wait | fire | interaction | aborted | done,
                          Description :: term()) ->
                           ok.

%% =============================================================================
%% API functions
%% =============================================================================

%% -----------------------------------------------------------------------------
%% Execute list of entries
%% -----------------------------------------------------------------------------

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% execute/2
%% ###### Purpose
%% Execute compiled workflow language
%% ###### Arguments
%%
%% ###### Returns
%% ###### Exceptions
%% throw : {parallel_errors, [error_tuplues], State}
%% throw : {exit, error, Message, State}
%% throw : {exit, ok, Message, State}
%% throw : {operation, Operation, State}
%% throw : {variable, Operation, State}
%% throw : {operator_arg, Operation, State}
%% throw : {invalid, bool_op, Operation, State}
%% throw : {invalid, eval_co, Operation, State}
%% throw : {not_found, retval, RetValParName, State}
%% throw : {not_found, variable, RetValParName, State}
%%-------------------------------------------------------------------
%%
%% @end

-spec execute(compiled_entry() | [compiled_entry()], engine_state()) ->
  {ok, engine_state()}.
execute([], #{impl := Impl} = State) ->
  ok = Impl:drop_state(State),
  {ok, State};
execute([Entry | RestEntry], State) ->
  {ok, NewState} = execute_entry(Entry, State),
  execute(RestEntry, NewState).

%% -----------------------------------------------------------------------------
%% Execute entry
%% -----------------------------------------------------------------------------

% execute one entry
-spec execute_entry(compiled_entry(), engine_state()) ->
  {ok, engine_state()}.
execute_entry({ID, _} = Entry, #{impl := Impl} = State) ->
  {ok, Result, NewState} = execute_entry(Entry,
                                         get_execution_result(ID, State),
                                         State),
  % store execution result (purpose: will not be executed again if execution was
  % aborted and restarted)
  NewState1 = store_execution_result(ID, Result, NewState),
  ok = Impl:save_state(NewState1),
  {ok, NewState1}.

-spec get_execution_result(binary(), engine_state()) ->
  execution_result() | undefined.
get_execution_result(ID, #{executed := Executed}) ->
  maps:get(ID, Executed, undefined).

-spec store_execution_result(binary(), term(), engine_state()) ->
  engine_state().
store_execution_result(ID, Result, #{executed := Executed} = State) ->
  State#{executed := Executed#{ID => Result}}.

%% -----------------------------------------------------------------------------
%% Execute entry implementations
%% -----------------------------------------------------------------------------

-spec execute_entry(compiled_entry(),
                    execution_result() | undefined, engine_state()) ->
                     {ok, execution_result(), engine_state()}.

% execute rule which was not executed before
execute_entry({ID, {rule, {LogicalExpression, Steps}}},
              undefined,
              #{impl := Impl} = State) ->
  LogicalExprID = <<ID/binary, "_le">>,

  {LogicalExprResult, NewState1} =
    case get_execution_result(LogicalExprID, State) of
      undefined ->
        {EvalResult, St1} = eval_le(LogicalExpression, State),
        % save result of logical expression
        St2 = store_execution_result(LogicalExprID, EvalResult, St1),
        ok = Impl:save_state(St2),
        {EvalResult, St2};
      StoredResult ->
        % logical expression already evaluated,
        % (execution aborted previously, and restarted again)
        {StoredResult, State}
    end,

  case LogicalExprResult of
    true ->
      {ok, St3} = execute(Steps, NewState1),
      {ok, true, St3};
    false ->
      {ok, false, NewState1}
  end;

% execute interaction call which was not executed before
execute_entry({_, {call, {InteractionID, ParameterSpec, ReturnValueSpec}}},
              undefined,
              #{impl := Impl} = State) ->
  Parameters = create_parameter_values(State, ParameterSpec),
  {ok, ReturnValues, NewState} = Impl:execute_interaction(State,
                                                          InteractionID,
                                                          Parameters),
  {ok, NewState1} = process_return_values(ReturnValueSpec,
                                          ReturnValues,
                                          NewState),
  {ok, true, NewState1};

% execute parallel interaction call which was not executed before
execute_entry({_, {parallel, ParallelInteractions}},
              undefined,
              State) ->
  parallel(ParallelInteractions, State);

% execute workflow command which was not executed before
execute_entry({_, {cmd, _} = Command}, undefined, State) ->
  {ok, NewState} = execute_cmd(Command, State),
  {ok, true, NewState};

% entry already executed
execute_entry(_, Result, State) ->
  {ok, Result, State}.

-spec parallel([compiled_interaction()], engine_state()) ->
  {ok, execution_result(), engine_state()}.
% execute parallel interaction calls
parallel(ParallelInteractions, #{impl := Impl} = State) ->
  ExecFun =
    fun({ID, _} = Entry, PState) ->
      Result = get_execution_result(ID, PState),
      {ID, execute_entry(Entry, Result, PState)}
    end,
  ExecutionResults = wms_common:fork(ExecFun, [State],
                                     ParallelInteractions, infinity),

  % merge executed command states and collect error
  {MergedState, Errors} =
    lists:foldl(
      fun
        ({ok, {ID, {ok, Result, _}}}, {CurrState, Errors}) ->
          NewState = store_execution_result(ID, Result, CurrState),
          ok = Impl:save_state(NewState),
          {merge_state(CurrState, NewState), Errors};

        ({_, Error}, {CurrState, Errors}) ->
          {CurrState, [Error | Errors]}
      end, {State, []}, ExecutionResults),

  case Errors of
    [] ->
      % no errors was found
      {ok, true, MergedState};
    _ ->
      throw({parallel_errors, lists:usort(Errors), MergedState})
  end.

-spec merge_state(engine_state(), engine_state()) ->
  engine_state().
merge_state(#{executed := Into} = State, #{executed := From}) ->
  State#{executed := maps:merge(Into, From)}.

% create parameter map of interactions calls
-spec create_parameter_values(engine_state(), [parameter_spec()]) ->
  [parameter_value()].
create_parameter_values(#{impl :=Impl} = State, ParameterSpec) ->
  lists:map(
    fun({ParID, Ref}) ->
      {ok, Value} = wms_state:eval_var(Ref, Impl, State),
      {ParID, Value}
    end, ParameterSpec).

% process return values of interaction and set variables by
% specification of return values
-spec process_return_values([return_value_spec()],
                            return_values(), engine_state()) ->
                             {ok, engine_state()}.
process_return_values(ReturnValueSpec, ReturnValues, #{impl := Impl} = State) ->
  {ok, lists:foldl(
    fun({RetValParName, Destination}, PState) ->
      case maps:is_key(RetValParName, ReturnValues) of
        true ->
          {ok, NState} = Impl:set_variable(PState,
                                           Destination,
                                           maps:get(RetValParName, ReturnValues),
                                           false),
          NState;
        false ->
          throw({not_found, retval, RetValParName, PState})
      end
    end, State, ReturnValueSpec)}.

%% -----------------------------------------------------------------------------
%% Execute command
%% -----------------------------------------------------------------------------

-spec execute_cmd(command(), engine_state()) ->
  {ok, engine_state()}.
execute_cmd({cmd, {error, VariableOrLiteral}}, #{impl:=Impl} = State) ->
  {ok, Message} = wms_state:eval_var(VariableOrLiteral, Impl, State),
  throw({exit, error, Message, State});
execute_cmd({cmd, {exit, VariableOrLiteral}}, #{impl:=Impl} = State) ->
  {ok, Message} = wms_state:eval_var(VariableOrLiteral, Impl, State),
  throw({exit, ok, Message, State});
execute_cmd({cmd, {wait, WaitType, EventIDS}}, #{impl := Impl} = State) ->
  ok = Impl:save_state(State),
  Impl:wait_events(State, WaitType, EventIDS);
execute_cmd({cmd, {fire, EventID}}, #{impl := Impl} = State) ->
  Impl:fire_event(State, EventID);
execute_cmd({cmd, {move, Source, Destination}}, State) ->
  move_variable(Source, Destination, State).

%% -----------------------------------------------------------------------------
%% Eval logical expression
%% -----------------------------------------------------------------------------

-spec eval_le(logical_expr(), engine_state()) ->
  {boolean(), engine_state()}.
eval_le([], State) ->
  % no logical expression, always true
  {true, State};
eval_le(LogicalExpression, State) ->
  eval_le(LogicalExpression, undefined, State).

-spec eval_le(logical_expr(), tree_state(), engine_state()) ->
  {boolean(), engine_state()}.
eval_le([], true, State) ->
  {true, State};
eval_le(_, false, State) ->
  {false, State};
eval_le([{BooleanOp, ComparatorExpression} | Rest], PrevBooleanValue, State) ->
  {ResultComparsion, NewState1} = eval_ce(ComparatorExpression, State),
  {ResultBooleanOp, NewState2} = eval_bo(BooleanOp,
                                         ResultComparsion,
                                         PrevBooleanValue, NewState1),
  eval_le(Rest, ResultBooleanOp, NewState2).

%% -----------------------------------------------------------------------------
%% Eval boolean expression
%% -----------------------------------------------------------------------------

-spec eval_bo(bool_op(), tree_state(), tree_state(), engine_state()) ->
  {boolean(), engine_state()}.
eval_bo('and', Logical1, Logical2, State) ->
  {Logical1 and Logical2, State};
eval_bo('or', Logical1, Logical2, State) ->
  {Logical1 or Logical2, State};
eval_bo('xor', Logical1, Logical2, State) ->
  {Logical1 xor Logical2, State};
eval_bo('set', Logical1, undefined, State) ->
  {Logical1, State};
eval_bo(Invalid, _, _, State) ->
  throw({invalid, bool_op, Invalid, State}).

%% -----------------------------------------------------------------------------
%% Eval comparator expression
%% -----------------------------------------------------------------------------

-spec eval_ce(comparator_expr(), engine_state()) ->
  {boolean(), engine_state()}.
eval_ce({VariableRef, ComparatorOp, VariableOrLiteral}, #{impl:=Impl} = State) ->
  {ok, Val1} = wms_state:eval_var(VariableRef, Impl, State),
  {ok, Val2} = wms_state:eval_var(VariableOrLiteral, Impl, State),
  eval_co(ComparatorOp, Val1, Val2, State).

%% -----------------------------------------------------------------------------
%% Eval comparator operator
%% -----------------------------------------------------------------------------

-spec eval_co(comparator_op(), term(), term(), engine_state()) ->
  {boolean(), engine_state()}.
eval_co('=', Val1, Val2, State) ->
  {Val1 =:= Val2, State};
eval_co('>', Val1, Val2, State) ->
  {Val1 > Val2, State};
eval_co('>=', Val1, Val2, State) ->
  {Val1 >= Val2, State};
eval_co('<', Val1, Val2, State) ->
  {Val1 < Val2, State};
eval_co('<=', Val1, Val2, State) ->
  {Val1 =< Val2, State};
eval_co('!=', Val1, Val2, State) ->
  {Val1 =/= Val2, State};
eval_co(Invalid, _, _, State) ->
  throw({invalid, eval_co, Invalid, State}).

%% -----------------------------------------------------------------------------
%% Move variable
%% -----------------------------------------------------------------------------

-spec move_variable(source() | {source(), operation()},
                    destination(), engine_state()) ->
                     {ok, engine_state()}.

move_variable(Source, Destination, #{impl := Impl} = State) ->
  try
    {ok, _NewState} = wms_state:move_var(Source, Destination, Impl, State)
  catch error : {badmatch, {error, R}} ->
    ?error("move_variable: ~p -> ~p", [Source, Destination]),
    throw(R)
  end.