%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, Attila Makra.
%%% @doc
%%% Egy task vegrehajtasaert felelos
%%% @end
%%% Created : 05. Nov 2019 11:43
%%%-------------------------------------------------------------------
-module(wms_engine_task_executor).
-author("Attila Makra").

-include("wms_engine_lang.hrl").
-include_lib("wms_logger/include/wms_logger.hrl").

%% API
-export([execute/3]).

-spec execute([rule()], binary(), binary()) ->
  ok | {error, term()}.
execute(Rules, TaskName, TaskInstanceID) ->
  {ok, Compiled} = wms_engine_precomp:compile(Rules),
  State =
    case wms_engine_db_state:load_state(#{task_instance_id => TaskInstanceID}) of
      not_found ->
        #{impl => wms_engine_lang_impl,
          task_name => TaskName,
          task_instance_id => TaskInstanceID,
          private => #{},
          executed => #{},
          event_received => []};
      Other ->
        Other
    end,
  wms_engine_lang_impl:log_task_status(State, started, undefined),
  try
    {ok, NewState} = Ret = wms_engine_lang:execute(Compiled, State),
    wms_engine_lang_impl:log_task_status(NewState, done, undefined),
    Ret
  catch
    C:E:St ->
      ?error("TSK-0018",
             "~s instance of ~s task was failed. ~s:~0p~n~0p",
             [TaskInstanceID, TaskName, C, E, St]),
      wms_engine_lang_impl:log_task_status(State, aborted, {C, E, St}),
      throw(E)
  end.
