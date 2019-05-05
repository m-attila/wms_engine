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

%% API
-export([execute/2]).

%% =============================================================================
%% Types
%% =============================================================================

%% =============================================================================
%% Callbacks
%% =============================================================================
-callback save_state(State :: engine_state()) ->
  ok.

-callback evaluate_variable(State :: engine_state(),
                            VariableRef :: variable_reference()) ->
                             {term(), engine_state()}.

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

-callback set_variable(State :: engine_state(),
                       VariableRef :: variable_reference(),
                       Literal :: literal()) ->
                        {ok, engine_state()}.

%% =============================================================================
%% API functions
%% =============================================================================

%% -----------------------------------------------------------------------------
%% Execute list of entries
%% -----------------------------------------------------------------------------

-spec execute(compiled_entry() | [compiled_entry()], engine_state()) ->
  {ok, engine_state()}.
execute([], State) ->
  {ok, State};
execute([Entry | RestEntry], State) ->
  {ok, NewState} = execute_entry(Entry, State),
  execute(RestEntry, NewState).

%% -----------------------------------------------------------------------------
%% Execute entry
%% -----------------------------------------------------------------------------
-spec execute_entry(compiled_entry(), engine_state()) ->
  {ok, engine_state()}.
execute_entry({ID, _} = Entry, #{impl := Impl} = State) ->
  {ok, Result, NewState} = execute_entry(Entry,
                                         get_execution_result(ID, State),
                                         State),
  NewState1 = store_execution_result(ID, Result, NewState),
  ok = Impl:save_state(NewState1),
  {ok, NewState1}.

-spec get_execution_result(integer(), engine_state()) ->
  execution_result() | undefined.
get_execution_result(ID, #{executed := Executed}) ->
  maps:get(ID, Executed, undefined).

-spec store_execution_result(integer(), term(), engine_state()) ->
  engine_state().
store_execution_result(ID, Result, #{executed := Executed} = State) ->
  State#{executed := Executed#{ID => Result}}.

%% -----------------------------------------------------------------------------
%% Execute entry implementations
%% -----------------------------------------------------------------------------

-spec execute_entry(compiled_entry(), execution_result() | undefined, engine_state()) ->
  {ok, execution_result(), engine_state()}.
execute_entry({ID, {rule, {LogicalExpression, Steps}}},
              undefined,
              #{impl := Impl} = State) ->
  LogicalExprID = ID - 1,

  {LogicalExprResult, NewState1} =
    case get_execution_result(LogicalExprID, State) of
      undefined ->
        {EvalResult, St1} = eval_le(LogicalExpression, State),
        % save result of logical expression
        St2 = store_execution_result(LogicalExprID, EvalResult, St1),
        ok = Impl:save_state(St2),
        {EvalResult, St2};
      StoredResult ->
        {StoredResult, State}
    end,

  case LogicalExprResult of
    true ->
      {ok, St3} = execute(Steps, NewState1),
      {ok, true, St3};
    false ->
      {ok, false, NewState1}
  end;
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
execute_entry({_, {parallel, ParallelInteractions}},
              undefined,
              State) ->
  parallel(ParallelInteractions, State);
execute_entry({_, {cmd, _} = Command}, undefined, State) ->
  {ok, NewState} = execute_cmd(Command, State),
  {ok, true, NewState};
execute_entry(_, Result, State) ->
  % already executed
  {ok, Result, State}.

-spec parallel([compiled_interaction()], engine_state()) ->
  {ok, execution_result(), engine_state()}.
parallel(ParallelInteractions, #{impl := Impl} = State) ->
  ExecFun =
    fun({ID, _} = Entry, PState) ->
      Result = get_execution_result(ID, PState),
      {ID, wms_test:test(fun()->execute_entry(Entry, Result, PState)end)}
    end,
  ExecutionResults = wms_common:fork(ExecFun, [State],
                                     ParallelInteractions, infinity),

  {MergedState, Errors} =
    lists:foldl(
      fun({ID, {ok, Result, _}}, {CurrState, Errors}) ->
        NewState = store_execution_result(ID, Result, CurrState),
        ok = Impl:save_state(NewState),
        {merge_state(CurrState, NewState), Errors};

         ({_, Error}, {CurrState, Errors}) ->
           {CurrState, [Error | Errors]}
      end, {State, []}, ExecutionResults),

  case Errors of
    [] ->
      {ok, true, MergedState};
    _ ->
      throw({parallel_errors, Errors})
  end.

-spec merge_state(engine_state(), engine_state()) ->
  engine_state().
merge_state(#{executed := Into} = State, #{executed := From}) ->
  State#{executed := maps:merge(Into, From)}.

-spec create_parameter_values(engine_state(), [parameter_spec()]) ->
  [parameter_value()].
create_parameter_values(State, ParameterSpec) ->
  lists:map(
    fun({ParID, Ref}) ->
      {Value, _} = eval_var(Ref, State),
      {ParID, Value}
    end, ParameterSpec).

-spec process_return_values([return_value_spec()], return_values(), engine_state()) ->
  {ok, engine_state()}.
process_return_values(ReturnValueSpec, ReturnValues, #{impl := Impl} = State) ->
  {ok, lists:foldl(
    fun({RetValParName, Destination}, PState) ->
      case maps:is_key(RetValParName, ReturnValues) of
        true ->
          {ok, NState} = Impl:set_variable(PState,
                                           Destination,
                                           maps:get(RetValParName, ReturnValues)),
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
execute_cmd({cmd, {error, VariableOrLiteral}}, State) ->
  throw({exit, error, eval_var(VariableOrLiteral, State), State});
execute_cmd({cmd, {exit, VariableOrLiteral}}, State) ->
  throw({exit, ok, eval_var(VariableOrLiteral, State), State});
execute_cmd({cmd, {wait, WaitType, EventIDS}}, #{impl := Impl} = State) ->
  Impl:wait_events(State, WaitType, EventIDS);
execute_cmd({cmd, {fire, EventID}}, #{impl := Impl} = State) ->
  Impl:fire_event(State, EventID);
execute_cmd({cmd, {move, _Source, _Destination}}, _State) ->
%%  move_variable(Source, Destination, State).
  throw(not_implemented).

%% -----------------------------------------------------------------------------
%% Eval logical expression
%% -----------------------------------------------------------------------------

-spec eval_le(logical_expr(), engine_state()) ->
  {boolean(), engine_state()}.
eval_le([], State) ->
  % no logical expression, always true
  {true, State};
eval_le(LogicalExpression, State) ->
  eval_le(LogicalExpression, true, State).

-spec eval_le(logical_expr(), boolean(), engine_state()) ->
  {boolean(), engine_state()}.
eval_le([], true, State) ->
  {true, State};
eval_le(_, false, State) ->
  {false, State};
eval_le([{BooleanOp, ComparatorExpression} | Rest], true, State) ->
  {ResultComparsion, NewState1} = eval_ce(ComparatorExpression, State),
  {ResultBooleanOp, NewState2} = eval_bo(BooleanOp, ResultComparsion, true, NewState1),
  eval_le(Rest, ResultBooleanOp, NewState2).

%% -----------------------------------------------------------------------------
%% Eval boolean expression
%% -----------------------------------------------------------------------------

-spec eval_bo(bool_op(), boolean(), boolean(), engine_state()) ->
  {boolean(), engine_state()}.
eval_bo('and', Logical1, Logical2, State) ->
  {Logical1 and Logical2, State};
eval_bo('or', Logical1, Logical2, State) ->
  {Logical1 or Logical2, State};
eval_bo('xor', Logical1, Logical2, State) ->
  {Logical1 xor Logical2, State};
eval_bo(Invalid, _, _, State) ->
  throw({invalid, bool_op, Invalid, State}).

%% -----------------------------------------------------------------------------
%% Eval comparator expression
%% -----------------------------------------------------------------------------

-spec eval_ce(comparator_expr(), engine_state()) ->
  {boolean(), engine_state()}.
eval_ce({VariableRef, ComparatorOp, VariableOrLiteral}, State) ->
  {Val1, NewState1} = eval_var(VariableRef, State),
  {Val2, NewState2} = eval_var(VariableOrLiteral, NewState1),
  eval_co(ComparatorOp, Val1, Val2, NewState2).

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
eval_co(Invalid, _, _, State) ->
  throw({invalid, eval_co, Invalid, State}).

%% -----------------------------------------------------------------------------
%% Eval variable reference
%% -----------------------------------------------------------------------------

-spec eval_var(variable_or_literal(), engine_state()) ->
  {term(), engine_state()}.
eval_var({_, _} = Reference, #{impl:=Impl} = State) ->
  Impl:evaluate_variable(State, Reference);
eval_var(Literal, State) ->
  {Literal, State}.

%% -----------------------------------------------------------------------------
%% Move variable
%% -----------------------------------------------------------------------------

%%move_variable({{global, _} = Source, Operation}, {global, _} = Destination, State) ->
%%  remote_move_variable(Source, Operation, Destination, State);
%%move_variable({Source, Operation}, Destination, State) ->
%%  local_move_variable(Source, Operation, Destination, State);
%%
%%move_variable({global, _} = Source, {global, _} = Destination, State) ->
%%  remote_move_variable(Source, Destination, State);
%%move_variable(Source, Destination, State) ->
%%  local_move_variable(Source, Destination, State);
%%move_variable(Source, Destination, State) ->
%%  throw({invalid, move_variable, {Source, Destination}, State}).
%%
%%
%%-spec local_move_variable(source(), operation(), destination(), engine_state()) ->
%%  {ok, engine_state()}.
%%local_move_variable(Source, Operation, Destination, State) ->
%%  {SourceVal, NewState1} = eval_var(Source, State),
%%  {OpVal, NewState2} = eval_operation(SourceVal, Operation, NewState1),
%%  set_var(Destination, OpVal, NewState2).
%%
%%-spec local_move_variable(source(), destination(), engine_state()) ->
%%  {ok, engine_state()}.
%%local_move_variable(Source, Destination, State) ->
%%  {SourceVal, NewState1} = eval_var(Source, State),
%%  set_var(Destination, SourceVal, NewState1).
%%
%%-spec remote_move_variable(variable_reference(), operation(), variable_reference(), engine_state()) ->
%%  {ok, engine_state()}.
%%remote_move_variable(_Source, _Operation, _Destination, _State) ->
%%  erlang:error(not_implemented).
%%
%%-spec remote_move_variable(variable_reference(), variable_reference(), engine_state()) ->
%%  {ok, engine_state()}.
%%remote_move_variable(_Source, _Destination, _State) ->
%%  erlang:error(not_implemented).
%%
%%-spec eval_operation(literal(), operation(), engine_state()) ->
%%  {literal(), engine_state()}.
%%eval_operation(_SourceVal, _Operation, _State) ->
%%  erlang:error(not_implemented).
%%
