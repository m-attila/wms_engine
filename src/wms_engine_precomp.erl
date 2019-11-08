%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%% Precompile WMS language.
%%% rule, rule_logikai kifejezés, call, parallel, cmd parancsokat
%%% egészíti ki hierarchikus binaris cimkekkel
%%% célja: hibakeresés
%%% a már lefutott logikai érték visszaszerzése, hogy a megszakadt,
%%% ismételt futás ne értékelje újra - tutira belemenjen
%%% a már lefutottatott végrehajtások eredményei meglegyenek.
%%% @end
%%% Created : 04. May 2019 10:00
%%%-------------------------------------------------------------------
-module(wms_engine_precomp).
-author("Attila Makra").

-include("wms_engine_lang.hrl").
%% API
-export([compile/1, get_ids/1]).

%% =============================================================================
%% Types
%% =============================================================================

-type counters() :: rule | call | parallel | cmd.

-type level_state() :: #{
level_id := binary(),
rule := integer(),
call := integer(),
parallel := integer(),
cmd := integer()
}.

-type state() :: #{
current_level := level_state(),
stack := [level_state()]
}.

%% =============================================================================
%% Functions
%% =============================================================================

% compile rules
-spec compile([rule()]) ->
  {ok, [compiled_rule()]}.
compile(Rules) ->
  State =
    #{
      current_level => new_level(),
      stack => []
    },

  {_, Compiled} = compile(Rules, [], State),
  {ok, lists:reverse(Compiled)}.

% compiles all entries hierarchical
-spec compile(entry() | [entry()],
              [compiled_entry()], state()) ->
               {state(), [compiled_entry()]}.
compile([], AccList, State) ->
  {State, AccList};

compile([Element | Rest], AccList, State) ->
  {NewState, NewAccList} = compile(Element, AccList, State),
  compile(Rest, NewAccList, NewState);

% compile rule
compile({rule, {LogicalExpression, Steps}}, AccList, State) ->
  {NewID, NewState} = new(rule, State),
  NewState1 = enter(NewID, NewState),

  % compile steps of rule
  {NewState2, CompiledSteps} = compile(Steps, [], NewState1),
  NewState3 = leave(NewState2),

  CompiledRule = {NewID,
                  {rule,
                   {LogicalExpression, lists:reverse(CompiledSteps)}}},
  {NewState3, [CompiledRule | AccList]};

% compile interaction call
compile({call, _} = Interaction, AccList, State) ->
  {NewID, NewState} = new(call, State),
  {NewState, [{NewID, Interaction} | AccList]};

% compile parallel interaction calls
compile({parallel, ParallelInteractions}, AccList, State) ->
  {NewID, NewState} = new(parallel, State),
  NewState1 = enter(NewID, NewState),

  {NewState2, CompiledInteractions} = compile(ParallelInteractions, [], NewState1),
  NewState3 = leave(NewState2),
  CompiledParallel = {NewID, {parallel,
                              lists:reverse(CompiledInteractions)}},
  {NewState3, [CompiledParallel | AccList]};

% compile workflow command
compile({cmd, _} = Command, AccList, State) ->
  {NewID, NewState} = new(cmd, State),
  {NewState, [{NewID, Command} | AccList]}.

% return all IDS of compiled entries
-spec get_ids([compiled_entry()]) ->
  [binary()].
get_ids(Compiled) ->
  get_ids(Compiled, []).

get_ids([], Accu) ->
  Accu;
get_ids([{ID, {rule, {_, Step}}} | Rest], Accu) ->
  get_ids(Rest, [ID, <<ID/binary, "_le">> | Accu] ++ get_ids(Step, []));
get_ids([{ID, {parallel, Interactions}} | Rest], Accu) ->
  get_ids(Rest, [ID | Accu] ++ get_ids(Interactions, []));
get_ids([{ID, _} | Rest], Accu) ->
  get_ids(Rest, [ID | Accu]).

-spec new(counters(), state()) ->
  {binary(), state()}.
new(Counter, #{current_level := Current} = State) ->
  LevelID = maps:get(level_id, Current),
  % new counter number for level
  N = maps:get(Counter, Current) + 1,

  NewID = <<(atom_to_binary(Counter, latin1))/binary, "@",
            (integer_to_binary(N))/binary>>,

  {case LevelID of
     <<>> ->
       NewID;
     _ ->
       <<LevelID/binary, "_", NewID/binary>>
   end,
   State#{current_level := Current#{Counter := N}}}.

% enter subentries of current level
-spec enter(binary(), state()) ->
  state().
enter(ID, #{current_level := Current, stack := Stack} = State) ->
  State#{
    current_level := new_level(ID),
    stack := [Current | Stack]
  }.

% leave current level, subentries are processed
leave(#{stack := [Top | Rest]} = State) ->
  State#{
    current_level := Top,
    stack := Rest
  }.

-spec new_level() ->
  level_state().
new_level() ->
  new_level(<<>>).

-spec new_level(binary()) ->
  level_state().
new_level(ID) ->
  #{
    level_id => ID,
    rule => 0,
    call => 0,
    parallel => 0,
    cmd => 0
  }.