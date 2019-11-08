%%%-------------------------------------------------------------------
%% @doc wms_engine public API
%% @end
%%%-------------------------------------------------------------------

-module(wms_engine_app).

-behaviour(application).
-include("wms_engine.hrl").

%% Application callbacks
-export([start/2,
         stop/1,
         init/0]).

%%====================================================================
%% API
%%====================================================================
-spec start(Type :: application:start_type(), Args :: term()) ->
  {ok, Pid :: pid()} |
  {error, Reason :: term()}.
start(_StartType, _StartArgs) ->
  ok = wms_cfg:start_apps(?APP_NAME, [wms_dist, wms_db, wms_events]),
  ok = init(),
  wms_engine_sup:start_link().

%%--------------------------------------------------------------------
-spec stop(any()) ->
  atom().
stop(_State) ->
  ok.

%%====================================================================
%% Internal functions
%%====================================================================

-spec init() ->
  ok.
init() ->
  ok.