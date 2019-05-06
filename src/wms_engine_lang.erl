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
%% throw : {invalid, eval_co, Operation, State}
%% throw : {invalid, bool_op, Operation, State}
%% throw : {not_found, retval, RetValParName, State}
%%-------------------------------------------------------------------
%%
%% @end

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
      {ID, execute_entry(Entry, Result, PState)}
    end,
  ExecutionResults = wms_common:fork(ExecFun, [State],
                                     ParallelInteractions, infinity),

  {MergedState, Errors} =
    lists:foldl(
      fun({ok, {ID, {ok, Result, _}}}, {CurrState, Errors}) ->
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
      throw({parallel_errors, lists:usort(Errors), MergedState})
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
  {Message, _} = eval_var(VariableOrLiteral, State),
  throw({exit, error, Message, State});
execute_cmd({cmd, {exit, VariableOrLiteral}}, State) ->
  {Message, _} = eval_var(VariableOrLiteral, State),
  throw({exit, ok, Message, State});
execute_cmd({cmd, {wait, WaitType, EventIDS}}, #{impl := Impl} = State) ->
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
eval_co('!=', Val1, Val2, State) ->
  {Val1 =/= Val2, State};
eval_co(Invalid, _, _, State) ->
  throw({invalid, eval_co, Invalid, State}).

%% -----------------------------------------------------------------------------
%% Eval variable reference
%% -----------------------------------------------------------------------------

-spec eval_var(variable_or_literal(), engine_state()) ->
  {term(), engine_state()}.
eval_var({private, _} = Reference, #{impl:=Impl} = State) ->
  Impl:evaluate_variable(State, Reference);
eval_var({global, _} = Reference, #{impl:=Impl} = State) ->
  Impl:evaluate_variable(State, Reference);
eval_var(Literal, State) ->
  {Literal, State}.

%% -----------------------------------------------------------------------------
%% Eval operation
%% -----------------------------------------------------------------------------
-define(TWO_ARG_OPS, #{
  '+' => fun plus_operator/3,
  '-' => fun minus_operator/3,
  '*' => fun multiple_operator/3,
  '/' => fun divisor_operator/3
}).

eval_operation(Literal, nop, State) ->
  {Literal, State};
eval_operation(Literal, {Operation, OpArg} = Op, State) ->
  {OpArgVal, NewState1} = eval_var(OpArg, State),
  case maps:get(Operation, ?TWO_ARG_OPS, undefined) of
    undefined ->
      throw({invalid, eval_operation, Op, State});
    Fun ->
      Fun(Literal, OpArgVal, NewState1)
  end;
eval_operation(_, Op, State) ->
  throw({invalid, eval_operation, Op, State}).

-spec plus_operator([term()] | number() | boolean(),
                    [term()] | number() | boolean(), engine_state()) ->
                     {[term()] | number()| boolean(), engine_state()}.
plus_operator(Literal, OpArgVal, State) when is_list(Literal)
                                             andalso is_list(OpArgVal) ->
  {Literal ++ OpArgVal, State};
plus_operator(Literal, OpArgVal, State) when is_number(Literal)
                                             andalso is_number(OpArgVal) ->
  {Literal + OpArgVal, State}.
plus_operator(Literal, OpArgVal, State) when is_boolean(Literal)
                                             andalso is_boolean(OpArgVal) ->
  {Literal or OpArgVal, State};
plus_operator(Literal, OpArgVal, State) ->
  throw({invalid, '+', {Literal, OpArgVal}, State}).

-spec minus_operator(number() | boolean() | [term()],
                     number() | boolean() | [term()], engine_state()) ->
                      {number()| boolean() | [term()], engine_state()}.
minus_operator(Literal, OpArgVal, State) when is_number(Literal)
                                              andalso is_number(OpArgVal) ->
  {Literal - OpArgVal, State}.
minus_operator(Literal, OpArgVal, State) when is_boolean(Literal)
                                              andalso is_boolean(OpArgVal) ->
  {Literal xor OpArgVal, State};
minus_operator(Literal, OpArgVal, State) when is_list(Literal)
                                              andalso is_list(OpArgVal) ->
  {lists:subtract(Literal, OpArgVal), State};
minus_operator(Literal, OpArgVal, State) ->
  throw({invalid, '-', {Literal, OpArgVal}, State}).

-spec multiple_operator(number() | boolean() | [term()],
                        number() | boolean() | [term()], engine_state()) ->
                         {number()| boolean()| [term()], engine_state()}.
multiple_operator(Literal, OpArgVal, State) when is_number(Literal)
                                                 andalso is_number(OpArgVal) ->
  {Literal * OpArgVal, State}.
multiple_operator(Literal, OpArgVal, State) when is_boolean(Literal)
                                                 andalso is_boolean(OpArgVal) ->
  {Literal and OpArgVal, State};
multiple_operator(Literal, OpArgVal, State) when is_list(Literal)
                                                 andalso is_list(OpArgVal) ->
  {lists:usort(
    lists:filter(
      fun(E) ->
        lists:member(E, OpArgVal) end, Literal) ++
      lists:filter(
        fun(E) ->
          lists:member(E, Literal) end, OpArgVal)),
   State};
multiple_operator(Literal, OpArgVal, State) ->
  throw({invalid, '*', {Literal, OpArgVal}, State}).

-spec divisor_operator(integer() | float(),
                       integer() | float(), engine_state()) ->
                        {integer() | float(), engine_state()}.
divisor_operator(Literal, OpArgVal, State) when is_integer(Literal)
                                                andalso is_integer(OpArgVal) ->
  {Literal div OpArgVal, State}.
divisor_operator(Literal, OpArgVal, State) when is_number(Literal)
                                                andalso is_number(OpArgVal) ->
  {Literal / OpArgVal, State};
divisor_operator(Literal, OpArgVal, State) ->
  throw({invalid, '/', {Literal, OpArgVal}, State}).


%% -----------------------------------------------------------------------------
%% Move variable
%% -----------------------------------------------------------------------------

-spec move_variable(source() | {source(), operation()}, destination(), engine_state()) ->
  {ok, engine_state()}.

move_variable(Source, Destination, State) ->
  move_variable(Source, Destination, State, is_remote(Source, Destination)).

-spec move_variable(source() | {source(), operation()}, destination(), engine_state(), boolean()) ->
  {ok, engine_state()}.
move_variable({{_, _} = Source, Op}, Destination, #{impl := Impl} = State, false) ->
  {SourceVal, NewState1} = eval_var(Source, State),
  {SourceVal1, NewState2} = eval_operation(SourceVal, Op, NewState1),
  {ok, _NewState3} = Impl:set_variable(NewState2, Destination, SourceVal1);
move_variable(Source, Destination, State, false) ->
  move_variable({{Source, nop}, Destination}, State, false);
move_variable(_, _, _, true) ->
  throw(not_implemented).

-spec is_remote(source() | {source(), operation()}, destination()) ->
  boolean().
is_remote({global, _ID1}, {global, _ID2}) ->
  true;
is_remote({{_, _ID1}, {_Op, {global, _OpArg}}}, {global, _}) ->
  true;
is_remote({{global, _ID1}, {_Op, {_, _OpArg}}}, {global, _}) ->
  true;
is_remote(_, _) ->
  false.

