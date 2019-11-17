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
-include_lib("wms_db/include/wms_db_datatypes.hrl").
-include_lib("wms_common/include/wms_common.hrl").

%% API
-export([init/0,
         restart_task_controller/1,
         start_task/2,
         notify/3,
         interaction_reply/5,
         keepalive/4,
         get_task_definition/2,
         get_all_task_definition/1,
         put_task_definition/2,
         put_task_definitions/2,
         delete_task_definition/2,
         get_running_task_instances/1,
         stop_task/2,
         select_global_variables/2,
         get_private_variables/2,
         set_global_variable/3]).

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
%% stop_task/1
%% ###### Purpose
%% Stop task instance
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec stop_task(map(), identifier_name()) ->
  {map(), ok | {error, term()}}.
stop_task(State, TaskInstanceID) ->
  Reply = wms_engine_task_controller:stop_task(TaskInstanceID),
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


%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% get_task_definition/2
%% ###### Purpose
%% Returns definition for given taskname
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec get_task_definition(map(), binary()) ->
  {map(), {ok, map() | {error, term()}}}.
get_task_definition(State, TaskName) ->
  Reply =
    case wms_db:get_taskdef(TaskName) of
      #taskdef{
        definition = Definition,
        type       = Type} ->
        {ok, #{name => TaskName,
               definition => Definition,
               type => Type}};
      Other ->
        ?error("ENG-0001", "Task definition for ~s does not exists", [TaskName]),
        {error, Other}
    end,
  {State, Reply}.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% get_all_task_definition/0
%% ###### Purpose
%% Return all stored task definitions
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec get_all_task_definition(map()) ->
  {map(), {ok, [map()]}}.
get_all_task_definition(State) ->
  AllTaskDefs =
    lists:map(
      fun(#taskdef{task_name = TaskName, definition = Definition, type = Type}) ->
        #{name => TaskName,
          definition => Definition,
          type => Type}
      end, wms_db:get_taskdefs()),
  {State, {ok, AllTaskDefs}}.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% put_task_definition/2
%% ###### Purpose
%% Insert or update task definition and start it
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec put_task_definition(map(), map()) ->
  {map(), ok | {error, term()}}.
put_task_definition(State, #{name := TaskName,
                             definition := Definition,
                             type := Type}) ->
  ok = wms_db:add_taskdef(TaskName, Definition, Type),
  ok = wms_engine_task_controller:add_task_definition(TaskName, Definition, Type),

  case Type of
    auto ->
      start_task(State, TaskName);
    _ ->
      {State, ok}
  end.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% put_task_definitions/2
%% ###### Purpose
%% Insert or update task definitions and start them
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec put_task_definitions(map(), [map()]) ->
  {map(), ok | {error, term()}}.
put_task_definitions(State, Definitions) ->
  Return =
    [put_task_definition(State, Def) || Def <- Definitions],

  case lists:any(
    fun({_, {_, {error, _}}}) ->
      true;
       (_) ->
         false
    end, Return) of
    true ->
      {State, {error, Return}};
    false ->
      {State, ok}
  end.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% delete_task_definition/2
%% ###### Purpose
%% Remove task definitions
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec delete_task_definition(map(), identifier_name()) ->
  {map, ok | {error, term()}}.
delete_task_definition(State, TaskName) ->
  ok = wms_db:remove_taskdef(TaskName),
  {State, wms_engine_task_controller:delete_task_definition(TaskName)}.

-spec get_running_task_instances(map()) ->
  {map(), {ok, [
  {
    identifier_name(),
    [
    {identifier_name(), {wms_engine_task_controller:task_running_status(), timestamp(), term()}}
    ]
  }
  ]}}.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% get_running_task_instances/1
%% ###### Purpose
%%
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
get_running_task_instances(State) ->
  {State, {ok, wms_engine_task_controller:get_running_task_instances()}}.


-spec select_global_variables(map(), binary()) ->
  {map(), {ok, map()}}.
select_global_variables(State, Pattern) ->
  {State, wms_db:filter_global_variables(Pattern)}.

-spec get_private_variables(map(), binary()) ->
  {map(), {ok, map()}}.
get_private_variables(State, TaskInstanceID) ->
  #{private := Private} = wms_db:load_private_state(TaskInstanceID),
  {State, {ok, Private}}.

-spec set_global_variable(map(), identifier_name(), literal()) ->
  {map(), ok}.
set_global_variable(State, VariableName, Value) ->
  {State, wms_db:set_global_variable(VariableName, Value)}.

%% =============================================================================
%% Private functions
%% =============================================================================
start_controller(State) ->
  {ok, Pid} = wms_engine_task_controller:start_link(),
  State#{task_controller => Pid}.

