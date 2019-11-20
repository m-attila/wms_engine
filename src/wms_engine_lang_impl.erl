%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%% wms_engine_lang behavior implementation
%%% @end
%%% Created : 29. Oct 2019 03:36
%%%-------------------------------------------------------------------
-module(wms_engine_lang_impl).
-author("Attila Makra").
-behavior(wms_engine_lang).

-include("wms_engine_lang.hrl").
-include_lib("wms_logger/include/wms_logger.hrl").

%% API
-export([get_variable/2,
         set_variable/4,
         transaction/2,
         save_state/1,
         execute_interaction/3,
         wait_events/3,
         fire_event/2,
         drop_state/1,
         load_state/1,
         log_task_status/3]).


%% -----------------------------------------------------------------------------
%% wms_state_variable callbacks
%% -----------------------------------------------------------------------------

% változó értékét adja vissza
-spec get_variable(Environment :: map(), Reference :: variable_reference()) ->
  {ok, Value :: literal()} | {error, Reason :: term()}.
get_variable(_Environment, {private, now}) ->
  {ok, calendar:local_time()};
get_variable(_Environment, {global, now}) ->
  {ok, calendar:local_time()};
get_variable(Environment, Reference) ->
  wms_engine_db_state:get_variable(Environment, Reference).

% változó értékét állítja be
-spec set_variable(Environment :: map(),
                   Reference :: variable_reference(),
                   Value :: literal(),
                   InTransaction :: boolean()) ->
                    {ok, NewEnvironment :: map()} | {error, Reason :: term()}.
set_variable(Environment, Reference, Value, InTransaction) ->
  wms_engine_db_state:set_variable(Environment, Reference, Value, InTransaction).

-spec transaction(StartEnvironment :: map(),
                  Transaction :: transaction_fun()) ->
                   {ok, map()} | {error, term()}.
transaction(StartEnvironment, Transaction) ->
  wms_engine_db_state:transaction(StartEnvironment, Transaction).


-spec save_state(State :: map()) ->
  ok.
save_state(State) ->
  wms_engine_db_state:save_state(State).

-spec drop_state(State :: map()) ->
  ok.
drop_state(State) ->
  wms_engine_db_state:drop_state(State).

-spec load_state(InitialState :: map()) ->
  map().
load_state(InitialState) ->
  SavedState = wms_engine_db_state:load_state(InitialState),
  maps:merge(InitialState, SavedState).


%% -----------------------------------------------------------------------------
%% wms_engine_lang callbacks
%% -----------------------------------------------------------------------------

-spec execute_interaction(State :: engine_state(),
                          InteractionID :: identifier_name(),
                          ParameterValues :: [parameter_value()]) ->
                           {ok, return_values(), engine_state()}.
execute_interaction(#{task_instance_id := TaskInstanceID} = State,
                    InteractionID, ParameterValues) ->

  wms_engine_task_controller:change_task_status(TaskInstanceID,
                                                wait_interaction,
                                                {InteractionID, ParameterValues}),

  try
    log_task_status(State, interaction, {InteractionID, ParameterValues}),
    InteractionRequestID = wms_common:generate_unique_id(64),

    ok = wms_dist:call(wms_distributor_actor,
                       interaction, [TaskInstanceID,
                                     InteractionID,
                                     InteractionRequestID,
                                     ParameterValues]),

    case receive_interaction_messages(InteractionRequestID) of
      {ok, ReturnValues} ->
        {ok, ReturnValues, State};
      Error ->
        ?error("TSK-0019",
               "~s interaction reply for ~s task instance return with error: ~0p",
               [InteractionID, TaskInstanceID, Error]),
        throw(Error)
    end
  after
    wms_engine_task_controller:change_task_status(TaskInstanceID,
                                                  running, undefined)

  end.

-spec wait_events(State :: engine_state(),
                  wait_type(),
                  EventIDS :: [identifier_name()]) ->
                   {ok, engine_state()}.
wait_events(#{task_instance_id := TaskInstanceID} = State, WaitType, OEventIDS) ->
  EventIDS = replace_built_in_event_ids(OEventIDS),
  wms_engine_task_controller:change_task_status(TaskInstanceID,
                                                wait_event,
                                                {WaitType, EventIDS}),


  try
    log_task_status(State, wait, {WaitType, EventIDS}),
    purge_event_messages(),

    [ok = wms_dist:call(wms_events_actor,
                        subscribe,
                        [wms_common:timestamp(), EventID, TaskInstanceID]) ||
      EventID <- EventIDS],

    receive_events(WaitType, EventIDS, State),

    NewState = State#{event_received := []},

    {ok, NewState}
  after
    wms_engine_task_controller:change_task_status(TaskInstanceID,
                                                  running, undefined)
  end.


-spec fire_event(State :: engine_state(),
                 EventID :: identifier_name()) ->
                  {ok, engine_state()}.
fire_event(State, EventID) ->
  log_task_status(State, fire, EventID),
  ok = wms_dist:call(wms_events_actor,
                     fire_event,
                     [wms_common:timestamp(), EventID]),
  {ok, State}.

-spec log_task_status(State :: engine_state(),
                      Type ::
                      started | wait | fire | interaction | aborted | done,
                      Description :: term()) ->
                       ok.
log_task_status(#{task_name := TaskName,
                  task_instance_id := TaskInstanceID},
                Type,
                Description) ->
  ok = wms_db:set_task_instance_status(TaskInstanceID, TaskName,
                                       Type, Description).


%% =============================================================================
%% Private functions
%% =============================================================================

-spec purge_event_messages() ->
  ok.
purge_event_messages() ->
  receive
    {event_fired, _} ->
      purge_event_messages()
  after 100 ->
    ok
  end.

-spec receive_events(wait_type(), [identifier_name()], map()) ->
  ok.
receive_events(any, EventIDS, #{event_received := []} = State) ->

  ReceivedEventID =
    receive
      {event_fired, EventID} ->
        EventID;
      stop ->
        throw(task_aborted)
    end,

  case lists:member(ReceivedEventID, EventIDS) of
    true ->
      ok;
    false ->
      receive_events(any, EventIDS, State)
  end;
receive_events(any, _, #{event_received := _}) ->
  % event already received
  ok;
receive_events(all, EventIDS, #{task_instance_id := TaskInstanceID,
                                task_name := TaskName,
                                event_received := AlreadyReceived} = State) ->
  ReceivedEventID =
    receive
      {event_fired, EventID} ->
        EventID
    end,

  NewAlreadyReceived =
    case lists:member(ReceivedEventID, EventIDS) andalso not
    lists:member(ReceivedEventID, AlreadyReceived) of
      true ->
        ?debug("~s task, ~s instance received event: ~s",
               [TaskName, TaskInstanceID, ReceivedEventID]),
        [ReceivedEventID | AlreadyReceived];
      false ->
        AlreadyReceived
    end,

  case lists:subtract(EventIDS, NewAlreadyReceived) of
    [] ->
      ok;
    _ ->
      NewState = State#{event_received := NewAlreadyReceived},
      save_state(NewState),
      receive_events(all, EventIDS, NewState)
  end.

-spec receive_interaction_messages(identifier_name()) ->
  {ok, return_values()} | {error, term()}.
receive_interaction_messages(InteractionRequestID) ->
  Keepalive = wms_cfg:get(wms_dist, keepalive, undefined) * 2,

  receive
    {interaction_reply, InteractionRequestID, Reply} ->
      Reply;
    {keepalive, InteractionRequestID} ->
      receive_interaction_messages(InteractionRequestID);
    stop ->
      throw(task_aborted)
  after
    Keepalive ->
      {error, broken}
  end.

-spec replace_built_in_event_ids([event_id() | {mandatory, event_id()} | timer]) ->
  [event_id() | {mandatory, event_id()}].
replace_built_in_event_ids(EventIDS) ->
  lists:map(
    fun
      (timer) ->
        ?TIMER_EVENT_ID;
      (Other) ->
        Other
    end, EventIDS).