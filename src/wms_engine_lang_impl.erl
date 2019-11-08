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
         load_state/1, log_task_status/3]).


%% -----------------------------------------------------------------------------
%% wms_state_variable callbacks
%% -----------------------------------------------------------------------------

% változó értékét adja vissza
-spec get_variable(Environment :: map(), Reference :: variable_reference()) ->
  {ok, Value :: literal()} | {error, Reason :: term()}.
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
execute_interaction(State, InteractionID, ParameterValues) ->
  log_task_status(State, interaction, {InteractionID, ParameterValues}),
  throw(not_impl).

-spec wait_events(State :: engine_state(),
                  wait_type(),
                  EventIDS :: [identifier_name()]) ->
                   {ok, engine_state()}.
wait_events(#{task_instance_id := TaskInstanceID} = State, WaitType, EventIDS) ->
  log_task_status(State, wait, {WaitType, EventIDS}),
  purge_event_messages(),

  [ok = wms_dist:call(wms_events_actor,
                      subscribe,
                      [wms_common:timestamp(), EventID, TaskInstanceID]) ||
    EventID <- EventIDS],

  receive_events(WaitType, EventIDS, State),

  NewState = State#{event_received := []},

  {ok, NewState}.


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
        EventID
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




