%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%% Precompile WMS language
%%% @end
%%% Created : 04. May 2019 10:00
%%%-------------------------------------------------------------------
-module(wms_engine_precomp).
-author("Attila Makra").

-include("wms_engine_lang.hrl").

%% API
-export([compile/1]).


-spec compile([rule()]) ->
  {ok, [compiled_rule()]}.
compile(Rules) ->
  {_, Compiled} = compile(Rules, [], 1),
  {ok, lists:reverse(Compiled)}.

-spec compile(entry() | [entry()],
              [compiled_entry()], integer()) ->
               {integer(), [compiled_entry()]}.
compile([], AccList, NewID) ->
  {NewID, AccList};
compile([Element | Rest], AccList, NewID) ->
  {NewID1, NewAccList} = compile(Element, AccList, NewID),
  compile(Rest, NewAccList, NewID1);
compile({rule, {LogicalExpression, Steps}}, AccList, NewID) ->
  {NewID1, CompiledSteps} = compile(Steps, [], NewID),
  CompiledRule = {NewID1 + 1,
                  {rule,
                   {LogicalExpression, lists:reverse(CompiledSteps)}}},
  {NewID1 + 2, [CompiledRule | AccList]};
compile({call, _} = Interaction, AccList, NewID) ->
  {NewID + 1, [{NewID, Interaction} | AccList]};
compile({parallel, ParallelInteractions}, AccList, NewID) ->
  {NewID1, CompiledInteractions} = compile(ParallelInteractions, [], NewID),
  CompiledParallel = {NewID1, {parallel,
                               lists:reverse(CompiledInteractions)}},
  {NewID1 + 1, [CompiledParallel | AccList]};
compile({cmd, _} = Command, AccList, NewID) ->
  {NewID + 1, [{NewID, Command} | AccList]}.
