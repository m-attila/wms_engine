%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%% wms_engine_actor modul, ami leader election-ra hasznalt,
%%% tehat inditja a vegrehajto egysegeket adott node-okon es
%%% nyilvantartja a futtato processeket
%%% @end
%%% Created : 29. Oct 2019 04:03
%%%-------------------------------------------------------------------
-module(wms_engine_actor).
-author("Attila Makra").

-include_lib("wms_logger/include/wms_logger.hrl").
-include_lib("wms_state/include/wms_state.hrl").

%% API
-export([init/0,
         restart_task_controller/1,
         start_task/2,
         notify/3,
         interaction_reply/5, keepalive/4]).

%% =============================================================================
%% Initialization
%% =============================================================================

-spec init() ->
  map().
init() ->
  start_controller(#{}).

%% =============================================================================
%% API functions
%% =============================================================================

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% restart_task_controller/1
%% ###### Purpose
%% Restart wms_engine_task_controller gen_server.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec restart_task_controller(map()) ->
  map().
restart_task_controller(#{task_controller := Pid} = State) ->
  gen_server:stop(Pid),
  {start_controller(State), ok}.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% start_task/2
%% ###### Purpose
%% Start task from given TaskName manually
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec start_task(map(), identifier_name()) ->
  {map(), ok | {error, term()}}.
start_task(State, TaskName) ->
  Reply = wms_engine_task_controller:manual_start_task(TaskName),
  {State, Reply}.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% notify/3
%% ###### Purpose
%% Notify subscriber TaskInstanceID from EventID event was fired.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec notify(map(), identifier_name(), identifier_name()) ->
  {map(), ok | {error, term()}}.
notify(State, EventID, TaskInstanceID) ->
  Reply = wms_engine_task_controller:event_fired(TaskInstanceID, EventID),
  {State, Reply}.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% interaction_reply/3
%% ###### Purpose
%% Process interactions's reply
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec interaction_reply(map(), identifier_name(), identifier_name(),
                        identifier_name(),
                        {ok, map()} | {error, term()}) ->
                         {map(), ok | {error, term()}}.
interaction_reply(State, TaskInstanceID, InteractionID, InteractionRequestID, Reply) ->
  {State, wms_engine_task_controller:interaction_reply(TaskInstanceID,
                                                       InteractionID,
                                                       InteractionRequestID,
                                                       Reply)}.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% keepalive/4
%% ###### Purpose
%%
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec keepalive(map(), identifier_name(), identifier_name(), identifier_name()) ->
  {map(), ok | {error, term()}}.
keepalive(State, TaskInstanceID, InteractionID, InteractionRequestID) ->
  {State, wms_engine_task_controller:keepalive(TaskInstanceID,
                                               InteractionID,
                                               InteractionRequestID)}.

%% =============================================================================
%% Private functions
%% =============================================================================
start_controller(State) ->
  {ok, Pid} = wms_engine_task_controller:start_link(),
  State#{task_controller => Pid}.
