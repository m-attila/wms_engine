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

-include("wms_engine_lang.hrl").
-include_lib("wms_logger/include/wms_logger.hrl").
-include_lib("wms_db/include/wms_db_datatypes.hrl").

%% API
-export([start_link/0,
         is_running_task/0,
         manual_start_task/1,
         event_fired/2]).
-export([init/1,
         handle_info/2,
         handle_call/3,
         handle_cast/2,
         execute_wrapper/4]).

%% =============================================================================
%% Private types
%% =============================================================================
-type phase() :: wait_for_db | load_definitions | start_aborted_tasks | started.

-record(state, {
  phase = wait_for_db :: phase(),
  taskdef_instances :: [{taskdef(), [binary()]}],
  task_processes = #{} :: #{
  pid() => binary(),
  binary() => pid()
  }
}).
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

handle_info({'EXIT', Pid, _}, State) ->
  {noreply, task_exited(Pid, State)};

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
  ?error("Cannot be start ~s task, because controller not initialized yet",
         [TaskName]),
  {reply, {error, not_initialized}, State};
handle_call({event_fired, TaskInstanceID, EventID}, _From,
            #state{phase = started} = State) ->
  {reply, process_incoming_event(TaskInstanceID, EventID, State), State};
handle_call({event_fired, _, EventID}, _From, State) ->
  ?warning("Not initialized yet, ~s event notification dropped", [EventID]),
  {reply, {error, not_initialized}, State}.

-spec handle_cast(Request :: any(), State :: state()) ->
  {noreply, State :: state()}.
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
  State#state{phase = started};
start_new_tasks([{#taskdef{type = auto} = TaskDef, []} | Rest], State) ->
  NewState = start_new_task(TaskDef, State),
  start_new_tasks(Rest, NewState);
start_new_tasks([_ | Rest], State) ->
  start_new_tasks(Rest, State).

%% -----------------------------------------------------------------------------
%% Start new task by manually
%% -----------------------------------------------------------------------------

-spec start_new_task_by_name(binary(), state()) ->
  {term(), state()}.
start_new_task_by_name(TaskName, State) ->
  case wms_db:get_taskdef(TaskName) of
    not_found ->
      ?error("~s task definition was not found", [TaskName]),
      {{error, not_found}, State};
    TaskDef ->
      {ok, start_new_task(TaskDef, State)}
  end.

-spec start_new_task(taskdef(), state()) ->
  state().
start_new_task(#taskdef{task_name = TaskName,
                        type      = disabled}, State) ->
  ?error("~s task was not started, because it is disabled", [TaskName]),
  State;
start_new_task(#taskdef{task_name  = TaskName,
                        definition = Rules}, State) ->
  TaskInstanceID = wms_common:generate_unique_id(64),

  Pid = spawn_link(?MODULE,
                   execute_wrapper,
                   [Rules, TaskName, TaskInstanceID, false]),

  ?info("~s task started", [TaskName]),
  add_task(TaskInstanceID, Pid, State).

-spec start_aborted_task(taskdef(), binary(), state()) ->
  state().
start_aborted_task(#taskdef{task_name  = TaskName,
                            definition = Rules}, TaskInstanceID, State) ->
  Pid = spawn_link(?MODULE,
                   execute_wrapper,
                   [Rules, TaskName, TaskInstanceID, true]),

  ?info("~s/~s instance was restarted", [TaskName, TaskInstanceID]),
  add_task(TaskInstanceID, Pid, State).

%% -----------------------------------------------------------------------------
%% Task registration
%% -----------------------------------------------------------------------------

-spec execute_wrapper([rule()], binary(), binary(), boolean()) ->
  no_return().
execute_wrapper(Rules, TaskName, TaskInstanceID, false) ->
  wms_db:add_task_instance(TaskName, TaskInstanceID),
  execute_wrapper(Rules, TaskInstanceID, TaskName);
execute_wrapper(Rules, TaskName, TaskInstanceID, true) ->
  execute_wrapper(Rules, TaskInstanceID, TaskName).

-spec execute_wrapper([rule()], binary(), binary()) ->
  no_return().
execute_wrapper(Rules, TaskInstanceID, TaskName) ->
  Result =
    try
      wms_engine_task_executor:execute(Rules, TaskName, TaskInstanceID),
      wms_db:remove_task_instance(TaskName, TaskInstanceID)
    catch
      _:Error:St ->
        ?error("~s/~s task stopped with error: ~p~n~p",
               [TaskName, TaskInstanceID, Error, St]),
        {error, Error}
    end,
  ?debug("~s/~s task stopped with result ~p", [TaskName, TaskInstanceID, Result]),
  exit(Result).

-spec add_task(binary(), pid(), state()) ->
  state().
add_task(TaskInstanceID, Pid, #state{task_processes = TaskProcesses} = State) ->
  State#state{task_processes = TaskProcesses#{
    TaskInstanceID => Pid,
    Pid => TaskInstanceID
  }}.

-spec rem_task(map(), pid(), binary()) ->
  map().
rem_task(TaskProcesses, Pid, TaskInstanceID) ->
  maps:remove(TaskInstanceID, (maps:remove(Pid, TaskProcesses))).


-spec task_exited(pid(), state()) ->
  state().
task_exited(Pid, #state{task_processes = TaskProcesses} = State) ->
  case maps:get(Pid, TaskProcesses, undefined) of
    undefined ->
      State;
    TaskInstanceID ->
      State#state{task_processes = rem_task(TaskProcesses, Pid, TaskInstanceID)}
  end.

-spec process_incoming_event(binary(), binary(), state()) ->
  ok | {error, not_found}.
process_incoming_event(TaskInstanceID,
                       EventID, #state{task_processes = TaskProcesses}) ->
  case maps:get(TaskInstanceID, TaskProcesses, undefined) of
    undefined ->
      ?warning("~s task instance ID does not exists", [TaskInstanceID]),
      {error, not_found};
    Pid ->
      ?debug("~s task instance was advised for event ~s",
             [TaskInstanceID, EventID]),
      Pid ! {event_fired, EventID},
      ok
  end.