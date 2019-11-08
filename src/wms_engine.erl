%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%% API module for wms_engine
%%% @end
%%% Created : 09. May 2019 21:48
%%%-------------------------------------------------------------------
-module(wms_engine).
-author("Attila Makra").

-include("wms_engine.hrl").

%% API
-export([load_config/0]).

-spec load_config() ->
  ok.
load_config() ->
  load_config(wms_cfg:get(?APP_NAME, load_config, true)).

%% =============================================================================
%% Private functions
%% =============================================================================

-spec load_config(boolean()) ->
  ok.
load_config(true) ->
  Path = filename:join(code:priv_dir(?APP_NAME), "wms_engine.config"),
  ok = wms_cfg:overload_config(wms_cfg:get_mode(), [Path]);
load_config(_) ->
  ok.

