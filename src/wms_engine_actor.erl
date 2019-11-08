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

%% API
-export([init/0,
         restart_task_controller/1,
         start_task/2,
         notify/3]).

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

-spec restart_task_controller(map()) ->
  map().
restart_task_controller(#{task_controller := Pid} = State) ->
  gen_server:stop(Pid),
  {start_controller(State), ok}.

-spec start_task(map(), binary()) ->
  {map(), ok | {error, term()}}.
start_task(State, TaskName) ->
  Reply = wms_engine_task_controller:manual_start_task(TaskName),
  {State, Reply}.

-spec notify(map(), binary(), binary()) ->
  {map(), ok | {error, term()}}.
notify(State, EventID, TaskInstanceID) ->
  Reply = wms_engine_task_controller:event_fired(TaskInstanceID, EventID),
  {State, Reply}.

%% =============================================================================
%% Private functions
%% =============================================================================
start_controller(State) ->
  {ok, Pid} = wms_engine_task_controller:start_link(),
  State#{task_controller => Pid}.
