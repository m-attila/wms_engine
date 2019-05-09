%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%% Test behaviour implementation for wms_engine_lang
%%% @end
%%% Created : 04. May 2019 17:49
%%%-------------------------------------------------------------------
-module(wms_engine_lang_adapter).
-author("Attila Makra").
-behaviour(wms_engine_lang).

-include("../src/wms_engine_lang.hrl").

%% API
-export([init/0,
         destroy/0,
         set/2,
         get_history/0]).

-export([save_state/1,
         execute_interaction/3,
         wait_events/3,
         fire_event/2,
         set_variable/4, get_variable/2, transaction/2]).

init() ->
  ?MODULE = ets:new(?MODULE, [set, public, named_table, {keypos, 1}]),
  ets:insert(?MODULE, {execution, []}).

destroy() ->
  ets:delete(?MODULE).

set(Key, Value) ->
  ets:insert(?MODULE, {Key, Value}).

get_history() ->
  [{execution, History}] = ets:lookup(?MODULE, execution),
  lists:reverse(History).

%% =============================================================================
%% wms_engine_lang behaviour
%% =============================================================================

-spec set_variable(State :: engine_state(),
                   VariableRef :: variable_reference(),
                   Literal :: literal(),
                   InTransation :: boolean()) ->
                    {ok, engine_state()}.
set_variable(State, VariableRef, Literal, _) ->
  ets:insert(?MODULE, {VariableRef, Literal}),
  {ok, State}.

-spec save_state(State :: engine_state()) ->
  ok.
save_state(State) ->
  ets:insert(?MODULE, {engine_state, State}),
  ok.

-spec get_variable(Environment :: map(),
                   Reference :: variable_reference()) ->
                    {ok, Value :: literal()} | {error, Reason :: term()}.
get_variable(State, VariableRef) ->
  case ets:lookup(?MODULE, VariableRef) of
    [] ->
      {error, {variable_not_found, VariableRef}};
    [{VariableRef, Value}] ->
      {ok, Value}
  end.

-spec execute_interaction(State :: engine_state(),
                          InteractionID :: identifier_name(),
                          ParameterValues :: [parameter_value()]) ->
                           {ok, return_values(), engine_state()}.
execute_interaction(State, InteractionID, ParameterValues) ->
  add_execution({call, InteractionID, ParameterValues}),
  case ets:lookup(?MODULE, InteractionID) of
    [] ->
      throw({no_interaction_reply, InteractionID});
    [{InteractionID, ReturnValues}] ->
      {ok, check_interaction_ret(ReturnValues), State}
  end.

-spec wait_events(State :: engine_state(),
                  wait_type(),
                  EventIDS :: [identifier_name()]) ->
                   {ok, engine_state()}.
wait_events(State, Type, EventIDS) ->
  add_execution({wait, Type, EventIDS}),
  {ok, State}.

-spec fire_event(State :: engine_state(),
                 EventID :: identifier_name()) ->
                  {ok, engine_state()}.
fire_event(State, EventID) ->
  add_execution({fire, EventID}),
  {ok, State}.

-spec transaction(StartEnvironment :: map(),
                  Transaction :: transaction_fun()) ->
                   {ok, map()} | {error, term()}.
transaction(StartEnvironment, Transaction) ->
  Transaction(StartEnvironment).




add_execution(Term) ->
  [{execution, History}] = ets:lookup(?MODULE, execution),
  ets:update_element(?MODULE, execution, {2, [Term | History]}).

check_interaction_ret(#{} = Map) ->
  Map;
check_interaction_ret(Error) ->
  throw(Error).