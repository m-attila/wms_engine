%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%%
%%% @end
%%% Created : 05. Nov 2019 08:52
%%%-------------------------------------------------------------------
-module(wms_engine_task_controller).
-author("Attila Makra").
-behaviour(gen_server).

-include("wms_engine.hrl").
-include("wms_engine_lang.hrl").
-include_lib("wms_logger/include/wms_logger.hrl").
-include_lib("wms_common/include/wms_common.hrl").
-include_lib("wms_db/include/wms_db_datatypes.hrl").

%% API
-export([start_link/0,
         is_running_task/0,
         manual_start_task/1,
         event_fired/2,
         interaction_reply/4,
         keepalive/3,
         delete_task_definition/1,
         add_task_definition/3,
         get_running_task_instances/0,
         change_task_status/3,
         stop_task/1]).
-export([init/1,
         handle_info/2,
         handle_call/3,
         handle_cast/2,
         execute_wrapper/5]).

%% =============================================================================
%% Private types
%% =============================================================================
-export_type([task_running_status/0]).

-type phase() :: wait_for_db | load_definitions | start_aborted_tasks | started.
-type task_running_status() :: running | wait_event | wait_interaction.

% @formatter:off
-record(state, {
  phase = wait_for_db :: phase(),
  % task's instances
  taskdef_instances :: [{taskdef(), [binary()]}],
  % running task instances and pids
  task_processes = #{} :: #{
    pid() => binary(),
    binary() => pid()
  },
  task_status = #{} :: #{
    binary() => {task_running_status(), timestamp(), term()}
  }
}).
% @formatter:on
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================


-spec start_link() ->
  {ok, pid()} | ignore | {error, {already_started, pid()} | term()}.
start_link() ->
  Ret = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
  ?info("stared."),
  Ret.

-spec is_running_task() ->
  boolean().
is_running_task() ->
  gen_server:call(?MODULE, is_running_task).

-spec manual_start_task(binary()) ->
  ok | {error, term()}.
manual_start_task(TaskName) ->
  gen_server:call(?MODULE, {manual_start_task, TaskName}).

-spec event_fired(binary(), binary()) ->
  ok | {error, term()}.
event_fired(TaskInstanceID, EventID) ->
  gen_server:call(?MODULE, {event_fired, TaskInstanceID, EventID}).

-spec interaction_reply(identifier_name(), identifier_name(),
                        identifier_name(),
                        {ok, map()} | {error, term()}) ->
                         ok | {error, term()}.
interaction_reply(TaskInstanceID, InteractionID, InteractionRequestID, Reply) ->
  gen_server:call(?MODULE,
                  {interaction_reply,
                   TaskInstanceID, InteractionID, InteractionRequestID, Reply}).

-spec keepalive(identifier_name(), identifier_name(), identifier_name()) ->
  ok | {error, term()}.
keepalive(TaskInstanceID, InteractionID, InteractionRequestID) ->
  gen_server:call(?MODULE,
                  {keepalive, TaskInstanceID, InteractionID, InteractionRequestID}).

-spec delete_task_definition(identifier_name()) ->
  ok | {error, term()}.
delete_task_definition(TaskName) ->
  gen_server:call(?MODULE, {delete_task_definition, TaskName}).

-spec add_task_definition(binary(), taskdef_type(), term()) ->
  ok.
add_task_definition(TaskName, Type, Definition) ->
  gen_server:call(?MODULE, {add_task_definition, TaskName, Type, Definition}).

-spec change_task_status(identifier_name(), task_running_status(), term()) ->
  ok.
change_task_status(TaskInstanceID, Status, Term) ->
  gen_server:cast(?MODULE, {change_task_status, TaskInstanceID, Status, Term}).

-spec get_running_task_instances() ->
  [{
    identifier_name(),
    [
    {identifier_name(), {task_running_status(), timestamp(), term()}}
    ]
  }].
get_running_task_instances() ->
  gen_server:call(?MODULE, get_running_task_instances).

-spec stop_task(identifier_name()) ->
  ok | {error, term()}.
stop_task(TaskInstanceID) ->
  gen_server:call(?MODULE, {stop_task, TaskInstanceID}).

%% =============================================================================
%% gen_server behaviour
%% =============================================================================

-spec init(Args :: term()) ->
  {ok, State :: state()}.
init(_) ->
  process_flag(trap_exit, true),
  self() ! start,
  {ok, #state{}}.

-spec handle_info(Info :: any(), State :: state()) ->
  {noreply, State :: state()}.

%% -----------------------------------------------------------------------------
%% Process next phase
%% -----------------------------------------------------------------------------

handle_info(start, State) ->
  {noreply, handle_phase(State)};

%% -----------------------------------------------------------------------------
%% Task process exited signal.
%% -----------------------------------------------------------------------------

handle_info({'EXIT', Pid, {TaskType, TaskName, _}}, State) ->
  case TaskType of
    auto ->
      spawn(
        fun() ->
          ?debug("~s auto type task was restarted", [TaskName]),
          wms_engine_task_controller:manual_start_task(TaskName)
        end);
    _ ->
      ok
  end,
  NewState = task_exited(Pid, State),
  {noreply, NewState};

handle_info(timer_event, #state{phase = started} = State) ->
  spawn(
    fun() ->
      wms_dist:call(wms_events_actor,
                    fire_event,
                    [wms_common:timestamp(), ?TIMER_EVENT_ID])
    end),

  setup_timer_event(),
  {noreply, State};


handle_info(Msg, State) ->
  ?warning("Unknown message: ~0p", [Msg]),
  {noreply, State}.

-spec handle_call(Info :: any(), From :: {pid(), term()}, State :: state()) ->
  {reply, term(), State :: state()}.
handle_call(is_running_task, _From, #state{task_processes = Processes} = State) ->
  {reply, map_size(Processes) =/= 0, State};

handle_call({manual_start_task, TaskName}, _Form,
            #state{phase = started} = State) ->
  {Reply, NewState} = start_new_task_by_name(TaskName, State),
  {reply, Reply, NewState};
handle_call({manual_start_task, TaskName}, _Form, State) ->
  ?error("TSK-0008",
         "Cannot be start ~s task, because controller not initialized yet",
         [TaskName]),
  {reply, {error, not_initialized}, State};

handle_call({event_fired, TaskInstanceID, EventID}, _From,
            #state{phase = started} = State) ->
  {reply, process_incoming_event(TaskInstanceID, EventID, State), State};
handle_call({event_fired, _, EventID}, _From, State) ->
  ?error("TSK-0009",
         "Controller was not initialized yet, ~s event notification dropped", [EventID]),
  {reply, {error, not_initialized}, State};

handle_call({interaction_reply,
             TaskInstanceID, InteractionID, InteractionRequestID, Reply},
            _From, #state{phase = started} = State) ->
  {reply, process_interaction_reply(TaskInstanceID,
                                    InteractionID,
                                    InteractionRequestID,
                                    Reply,
                                    State), State};
handle_call({interaction_reply,
             TaskInstanceID, InteractionID, _InteractionRequestID, _Reply},
            _From, State) ->
  ?error("TSK-0010",
         "Controller was not initialized yet, ~s interaction "
         "reply was dropped for ~s task instance",
         [InteractionID, TaskInstanceID]),
  {reply, {error, not_initialized}, State};

handle_call({keepalive,
             TaskInstanceID, InteractionID, InteractionRequestID},
            _From, #state{phase = started} = State) ->
  {reply, process_keepalive(TaskInstanceID,
                            InteractionID,
                            InteractionRequestID,
                            State), State};
handle_call({keepalive,
             _TaskInstanceID, InteractionID, _InteractionRequestID, _Reply},
            _From, State) ->
  ?warning("Not initialized yet, keepalive was dropped ~s",
           [InteractionID]),
  {reply, {error, not_initialized}, State};

handle_call({delete_task_definition, TaskName}, _From,
            #state{taskdef_instances = Instances} = State) ->
  NewInstances =
    lists:filter(
      fun({#taskdef{task_name = N}, _}) ->
        N =/= TaskName
      end, Instances),
  Reply =
    case Instances of
      NewInstances ->
        ?error("TSK-0011",
               "~s task cannot be deleted, because does not found", [TaskName]),
        {error, not_found};
      _ ->
        ?info("~s task definition was deleted", [TaskName]),
        ok
    end,
  {reply, Reply, State#state{taskdef_instances = NewInstances}};

handle_call({add_task_definition, TaskName, Type, Definition}, _From,
            #state{taskdef_instances = Instances} = State) ->

  IsExists =
    lists:any(
      fun({#taskdef{task_name = Name}, _}) ->
        Name =:= TaskName
      end, Instances),

  NewInstances =
    case IsExists of
      true ->
        ?info("Task definition was modified: ~s", [TaskName]),
        lists:map(
          fun
            ({#taskdef{task_name = Name} = TD, Inst}) when Name =:= TaskName ->
              {TD#taskdef{type = Type, definition = Definition}, Inst};
            (Other) ->
              Other
          end, Instances);
      false ->
        ?info("New task definition inserted: ~s", [TaskName]),
        [{                          #taskdef{
          task_name  = TaskName,
          type       = Type,
          definition = Definition}, []} | Instances]
    end,

  {reply, ok, State#state{taskdef_instances = NewInstances}};

handle_call(get_running_task_instances, _From,
            #state{taskdef_instances = Instances,
                   task_status       = Status} = State) ->

  Reply = lists:map(
    fun({#taskdef{task_name = Name}, Inst}) ->
      {Name,
       lists:map(
         fun(TaskInstanceID) ->
           {TaskInstanceID, maps:get(TaskInstanceID, Status, undefined)}
         end,
         Inst)}
    end, Instances),
  {reply, Reply, State};

handle_call({stop_task, TaskInstanceID}, _From,
            #state{task_processes = Processes} = State) ->
  Reply =
    case maps:get(TaskInstanceID, Processes, undefined) of
      undefined ->
        ?error("TSK-0012", "Cannot be stop ~s task instance, it is not running",
               [TaskInstanceID]),
        {error, not_found};
      Pid ->
        ?debug("stop command was send to ~s task instance", [TaskInstanceID]),
        Pid ! stop,
        ok
    end,
  {reply, Reply, State}.

-spec handle_cast(Request :: any(), State :: state()) ->
  {noreply, State :: state()}.

handle_cast({change_task_status, TaskInstanceID, TaskStatus, Term},
            #state{task_status = Status} = State) ->
  NewStatus = Status#{TaskInstanceID => {TaskStatus, wms_common:timestamp(), Term}},
  {noreply, State#state{task_status = NewStatus}};

handle_cast(_, State) ->
  {noreply, State}.

%% =============================================================================
%% Private functions
%% =============================================================================

%% -----------------------------------------------------------------------------
%% Phase handling
%% -----------------------------------------------------------------------------

handle_phase(#state{phase = wait_for_db} = State) ->
  case wms_db_handler_service:is_initialized() of
    true ->
      ?info("Phase end: wait_for_db"),
      self() ! start,
      State#state{phase = load_definitions};
    false ->
      erlang:send_after(1000, self(), start),
      State
  end;
handle_phase(#state{phase = load_definitions} = State) ->
  try
    DefsAndInstances = wms_db:get_taskdef_instances(),
    ?info("Phase end: load_definitions"),
    self() ! start,
    State#state{taskdef_instances = DefsAndInstances,
                phase             = start_aborted_tasks}
  catch
    _:_ ->
      erlang:send_after(1000, self(), start),
      State
  end;
handle_phase(#state{phase             = start_aborted_tasks,
                    taskdef_instances = DefsAndInstances} = State) ->
  self() ! start,
  start_aborted_tasks(DefsAndInstances, State);
handle_phase(#state{phase             = start_new_tasks,
                    taskdef_instances = DefsAndInstances} = State) ->
  start_new_tasks(DefsAndInstances, State).

%% -----------------------------------------------------------------------------
%% Starting tasks
%% -----------------------------------------------------------------------------


%% -----------------------------------------------------------------------------
%% Aborted tasks
%% -----------------------------------------------------------------------------

-spec start_aborted_tasks([{task(), [binary()]}], state()) ->
  state().
start_aborted_tasks([], State) ->
  ?info("Phase end: start_aborted_tasks"),
  State#state{phase = start_new_tasks};
start_aborted_tasks([{#taskdef{task_name = TaskName}, []} | Rest], State) ->
  ?debug("No aborted tasks for ~s", [TaskName]),
  start_aborted_tasks(Rest, State);
start_aborted_tasks([{TaskDef, TaskInstances} | Rest], State) ->
  NewState = start_aborted_task_instances(TaskDef, TaskInstances, State),
  start_aborted_tasks(Rest, NewState).

-spec start_aborted_task_instances(taskdef(), [binary()], state()) ->
  state().
start_aborted_task_instances(#taskdef{task_name = TaskName}, [], State) ->
  ?info("All instances was restarted for task ~s", [TaskName]),
  State;
start_aborted_task_instances(#taskdef{task_name = TaskName} = TaskDef,
                             [TaskInstanceID | Rest], State) ->
  ?info("~s/~s instance was restarted", [TaskName, TaskInstanceID]),
  NewState = start_aborted_task(TaskDef, TaskInstanceID, State),
  start_aborted_task_instances(TaskDef, Rest, NewState).


%% -----------------------------------------------------------------------------
%% Start new task by automatically
%% -----------------------------------------------------------------------------

-spec start_new_tasks([taskdef()], state()) ->
  state().
start_new_tasks([], State) ->
  ?info("Phase end: start_new_tasks"),
  setup_timer_event(),
  State#state{phase = started};
start_new_tasks([{#taskdef{type = auto} = TaskDef, []} | Rest], State) ->
  NewState = start_new_task(TaskDef, State),
  start_new_tasks(Rest, NewState);
start_new_tasks([_ | Rest], State) ->
  start_new_tasks(Rest, State).

-spec setup_timer_event() ->
  any().
setup_timer_event() ->
  TimerEvent = wms_cfg:get(?APP_NAME, timer_event_delay, 30000),
  erlang:send_after(TimerEvent, self(), timer_event).

%% -----------------------------------------------------------------------------
%% Start new task by manually
%% -----------------------------------------------------------------------------

-spec start_new_task_by_name(binary(), state()) ->
  {term(), state()}.
start_new_task_by_name(TaskName, State) ->
  case wms_db:get_taskdef(TaskName) of
    not_found ->
      ?error("TSK-0013", "Cannot be start ~s task, it is not found", [TaskName]),
      {{error, not_found}, State};
    TaskDef ->
      {ok, start_new_task(TaskDef, State)}
  end.

-spec start_new_task(taskdef(), state()) ->
  state().
start_new_task(#taskdef{task_name = TaskName,
                        type      = disabled}, State) ->
  ?error("TSK-0014",
         "Cannot be start ~s task, task it is disabled", [TaskName]),
  State;
start_new_task(#taskdef{task_name  = TaskName,
                        definition = Rules,
                        type       = Type} = TD,
               #state{taskdef_instances = Instances} = State) ->

  DisableStart =
    case Type of
      auto ->
        lists:any(
          fun({#taskdef{task_name = N}, Inst}) ->
            N =:= TaskName andalso Inst =/= []
          end, Instances);
      _ ->
        false
    end,

  case DisableStart of
    false ->
      TaskInstanceID = wms_common:generate_unique_id(64),

      Pid = spawn_link(?MODULE,
                       execute_wrapper,
                       [Rules, Type, TaskName, TaskInstanceID, false]),

      ?info("~s task started", [TaskName]),
      add_task(TaskName, TaskInstanceID, Pid, State);
    true ->
      ?info("~s auto type task already running", [TaskName]),
      State
  end.


-spec start_aborted_task(taskdef(), binary(), state()) ->
  state().
start_aborted_task(#taskdef{task_name  = TaskName,
                            definition = Rules,
                            type       = Type}, TaskInstanceID, State) ->
  Pid = spawn_link(?MODULE,
                   execute_wrapper,
                   [Rules, Type, TaskName, TaskInstanceID, true]),

  ?info("~s/~s instance was restarted", [TaskName, TaskInstanceID]),
  add_task(TaskName, TaskInstanceID, Pid, State).

%% -----------------------------------------------------------------------------
%% Task registration
%% -----------------------------------------------------------------------------

-spec execute_wrapper([rule()], taskdef_type(), binary(), binary(), boolean()) ->
  no_return().
execute_wrapper(Rules, TaskType, TaskName, TaskInstanceID, false) ->
  wms_db:add_task_instance(TaskName, TaskInstanceID),
  execute_wrapper(Rules, TaskType, TaskInstanceID, TaskName);
execute_wrapper(Rules, TaskType, TaskName, TaskInstanceID, true) ->
  execute_wrapper(Rules, TaskType, TaskInstanceID, TaskName).

-spec execute_wrapper([rule()], taskdef_type(), binary(), binary()) ->
  no_return().
execute_wrapper(Rules, TaskType, TaskInstanceID, TaskName) ->
  Result =
    try
      wms_engine_task_executor:execute(Rules, TaskName, TaskInstanceID),
      wms_db:remove_task_instance(TaskName, TaskInstanceID)
    catch
      _:Error:St ->
        ?error("TSK-0015",
               "~s instance of ~s task stopped with error: ~0p~n~0p",
               [TaskInstanceID, TaskName, Error, St]),
        {error, Error}
    end,
  ?debug("~s/~s task stopped with result ~p", [TaskName, TaskInstanceID, Result]),
  exit({TaskType, TaskName, Result}).

-spec add_task(binary(), binary(), pid(), state()) ->
  state().
add_task(TaskName, TaskInstanceID, Pid, #state{task_processes    = TaskProcesses,
                                               taskdef_instances = Instances,
                                               task_status       = TaskStatus} = State) ->

  NewInstances =
    lists:map(
      fun
        ({#taskdef{task_name = N} = TD, Inst} = Entry) when N =:= TaskName ->
          case lists:member(TaskInstanceID, Inst) of
            true ->
              Entry;
            false ->
              {TD, [TaskInstanceID | Inst]};
            (Other) ->
              Other
          end;
        (Other) ->
          Other
      end, Instances),

  TS = {running, wms_common:timestamp(), undefined},

  State#state{task_processes    = TaskProcesses#{TaskInstanceID => Pid,
                                                 Pid => TaskInstanceID},
              taskdef_instances = NewInstances,
              task_status       = TaskStatus#{TaskInstanceID => TS}}.

-spec rem_task(pid(), binary(), state()) ->
  map().
rem_task(Pid, TaskInstanceID,
         #state{task_processes    = TaskProcesses,
                taskdef_instances = Instances,
                task_status       = TaskStatus} = State) ->
  NewTaskProcesses = maps:remove(TaskInstanceID, (maps:remove(Pid, TaskProcesses))),
  NewInstances =
    lists:map(
      fun({Def, Inst}) ->
        {Def, lists:delete(TaskInstanceID, Inst)}
      end, Instances),
  State#state{
    task_processes    = NewTaskProcesses,
    taskdef_instances = NewInstances,
    task_status       = maps:remove(TaskInstanceID, TaskStatus)
  }.

-spec task_exited(pid(), state()) ->
  state().
task_exited(Pid, #state{task_processes = TaskProcesses} = State) ->
  case maps:get(Pid, TaskProcesses, undefined) of
    undefined ->
      State;
    TaskInstanceID ->
      rem_task(Pid, TaskInstanceID, State)
  end.

-spec process_incoming_event(binary(), binary(), state()) ->
  ok | {error, not_found}.
process_incoming_event(TaskInstanceID,
                       EventID, #state{task_processes = TaskProcesses}) ->
  case maps:get(TaskInstanceID, TaskProcesses, undefined) of
    undefined ->
      ?error("TSK-0016",
        "~s task instance ID does not exists, ~s event "
        "will not be delivered", [TaskInstanceID, EventID]),
      {error, not_found};
    Pid ->
      ?debug("~s task instance was advised for event ~s",
             [TaskInstanceID, EventID]),
      Pid ! {event_fired, EventID},
      ok
  end.

-spec process_interaction_reply(identifier_name(), identifier_name(), identifier_name(), map(), state()) ->
  ok | {error, not_found}.
process_interaction_reply(TaskInstanceID, InteractionID, InteractionRequestID, Reply,
                          #state{task_processes = TaskProcesses}) ->
  case maps:get(TaskInstanceID, TaskProcesses, undefined) of
    undefined ->
      ?error("TSK-0017",
             "~s task instance ID does not exists, ~s request of ~s "
             "interaction will not be delivered: ~0p",
             [TaskInstanceID, InteractionRequestID, InteractionID, Reply]),
      {error, not_found};
    Pid ->
      ?debug("~s task instance was advised for ~s interaction result: ~p",
             [TaskInstanceID, InteractionID, Reply]),
      Pid ! {interaction_reply, InteractionRequestID, Reply},
      ok
  end.

process_keepalive(TaskInstanceID, InteractionID, InteractionRequestID,
                  #state{task_processes = TaskProcesses}) ->
  case maps:get(TaskInstanceID, TaskProcesses, undefined) of
    undefined ->
      ?warning("~s task instance ID does not exists", [TaskInstanceID]),
      {error, not_found};
    Pid ->
      ?debug("~s task instance was advised for ~s keepalive",
             [TaskInstanceID, InteractionID]),
      Pid ! {keepalive, InteractionRequestID},
      ok
  end.
