%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%% wms_state database integration.
%%% @end
%%% Created : 09. May 2019 08:05
%%%-------------------------------------------------------------------
-module(wms_engine_db_state).
-author("Attila Makra").
-behaviour(wms_state_variables).

-include_lib("wms_logger/include/wms_logger.hrl").
-include_lib("wms_state/include/wms_state.hrl").
-include("wms_engine_lang.hrl").
%% API
-export([get_variable/2,
         set_variable/4,
         transaction/2,
         save_state/1,
         drop_state/1,
         load_state/1]).

%% =============================================================================
%% wms_state_variables behaviour implementation
%% =============================================================================

-spec get_variable(Environment :: engine_state(),
                   Reference :: variable_reference()) ->
                    {ok, Value :: literal()} | {error, Reason :: term()}.

get_variable(#{private := PrivateVars,
               task_name := TaskName,
               task_instance_id := TaskInstanceID} = Environment, {private, VariableID}) ->
  % private variables are stored in Environment
  % {not_found, variable, RetValParName, State}
  case maps:get(VariableID, PrivateVars, undefined) of
    undefined ->
      ?error("TSK-0001", "~s private variable does not exists, for ~s task,
      in instance ~s.",
             [VariableID, TaskName, TaskInstanceID]),
      {error, {not_found, variable, VariableID, Environment}};
    Value ->
      {ok, Value}
  end;
get_variable(#{task_name := TaskName,
               task_instance_id := TaskInstanceID} =Environment, {global, VariableID}) ->
  % global variables are stored in database
  case wms_db:get_global_variable(VariableID) of
    not_found ->
      ?error("TSK-0002", "~s global variable does not exists, for ~s task,
      in instance ~s.",
             [VariableID, TaskName, TaskInstanceID]),
      {error, {not_found, variable, VariableID, Environment}};
    Value ->
      {ok, Value}
  end.

-spec set_variable(Environment :: engine_state(),
                   Reference :: variable_reference(),
                   Value :: literal(),
                   InTransaction :: boolean()) ->
                    {ok, NewEnvironment :: map()} | {error, Reason :: term()}.
set_variable(#{private := PrivateVars} = Environment, {private, VariableID},
             Value, _) ->
  % private variables are stored in Environment
  {ok, Environment#{private := PrivateVars#{VariableID => Value}}};
set_variable(Environment, {global, VariableID}, Value, _) ->
  {wms_db:set_global_variable(VariableID, Value), Environment}.

-spec transaction(StartEnvironment :: map(),
                  Transaction :: transaction_fun()) ->
                   {ok, map()} | {error, term()}.
transaction(StartEnvironment, Transaction) ->
  Ret = wms_db_handler:transaction(
    fun() ->
      Transaction(StartEnvironment)
    end),

  case Ret of
    {ok, Result} ->
      Result;
    _ ->
      Ret
  end.

-spec save_state(State :: engine_state()) ->
  ok.
save_state(#{task_instance_id :=TaskInstanceID} = State) ->
  wms_db:save_private_state(TaskInstanceID, State).

-spec drop_state(State :: engine_state()) ->
  ok.
drop_state(#{task_instance_id :=TaskInstanceID}) ->
  wms_db:remove_private_state(TaskInstanceID).

-spec load_state(InitialState :: map()) ->
  not_found | map().
load_state(#{task_instance_id :=TaskInstanceID}) ->
  wms_db:load_private_state(TaskInstanceID).





