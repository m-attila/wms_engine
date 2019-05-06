%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%%
%%% @end
%%% Created : 04. May 2019 17:48
%%%-------------------------------------------------------------------
-module(wms_engine_lang_test).
-author("Attila Makra").

-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Test functions
%% =============================================================================

execute_test() ->
  Var1 = {private, <<"var1">>},
  Var2 = {private, <<"var2">>},
  Var1Value = 1,
  Var2Value = 2,
  Par0101Value = 10,
  Par2101Value = 21,
  Par2201Value = 22,
  Ret01Result = <<"this is the result-01">>,
  Ret21Result = <<"this is the result-21">>,
  Ret22Result = <<"this is the result-22">>,
  Ret30Result = <<"this is the result-30">>,
  Ret01DestinationVariable = {private, <<"result01">>},
  Ret21DestinationVariable = {private, <<"result21">>},
  Ret22DestinationVariable = {private, <<"result22">>},
  Ret30DestinationVariable = {private, <<"result03">>},

  Rules = [
            {rule, {
              [
                {'set', {Var1, '>=', 1}},
                {'and', {Var1, '<=', 10}}
              ],
              [
                {call, {<<"int01">>,
                        [{<<"par01_01">>, Par0101Value},
                         {<<"par01_02">>, Var2}],
                        [{<<"ret01">>, Ret01DestinationVariable}]}
                },
                {parallel, [
                  {call, {<<"int21">>,
                          [{<<"par21_01">>, Par2101Value}],
                          [{<<"ret21">>, Ret21DestinationVariable}]}},
                  {call, {<<"int22">>,
                          [{<<"par22_01">>, Par2201Value}],
                          [{<<"ret22">>, Ret22DestinationVariable}]}}
                ]},
                {rule, {
                  [{'set', {Var1, '<', 1}}],
                  [
                    {call, {<<"int02">>,
                            [],
                            [{<<"ret01">>, Ret01DestinationVariable}]}}
                  ]
                }},
                {rule, {
                  [{'set', {Var2, '=', 2}}],
                  [
                    {call, {<<"int30">>,
                            [],
                            [{<<"ret30">>, Ret30DestinationVariable}]}}
                  ]
                }}
              ]
            }}
          ],

  Test =
    fun() ->
      {ok, Compiled} = wms_engine_precomp:compile(Rules),
      AllID = wms_engine_precomp:get_ids(Compiled),
      State = #{impl => wms_engine_lang_adapter, executed => #{}},
      {ok, NewState} = wms_engine_lang:execute(Compiled, State),

      % execute with result true
      assert_exec_result(
        NewState, AllID,
        [<<"rule@1">>, <<"rule@1_le">>, <<"rule@1_call@1">>,
         <<"rule@1_parallel@1">>,
         <<"rule@1_parallel@1_call@1">>, <<"rule@1_parallel@1_call@2">>,
         <<"rule@1_rule@2">>,
         <<"rule@1_rule@2_le">>, <<"rule@1_rule@2_call@1">>], true),

      % executed with result false
      assert_exec_result(
        NewState, AllID,
        [<<"rule@1_rule@1">>, <<"rule@1_rule@1_le">>], false),

      % not executed
      assert_exec_result(
        NewState, AllID,
        [<<"rule@1_rule@1_call@1">>], undefined),

      % check interaction
      ExpectedHistory =
        [
          {call,
           <<"int01">>,
           [{<<"par01_01">>, Par0101Value}, {<<"par01_02">>, Var2Value}]},
          {call,
           <<"int21">>,
           [{<<"par21_01">>, Par2101Value}]},
          {call,
           <<"int22">>,
           [{<<"par22_01">>, Par2201Value}]},
          {call,
           <<"int30">>,
           []}
        ],
      [Seq | Parallels] = wms_engine_lang_adapter:get_history(),
      ?assertEqual(ExpectedHistory, [Seq | lists:sort(Parallels)]),

      % check interaction return value
      ?assertMatch(
        {Ret01Result, _},
        wms_engine_lang_adapter:evaluate_variable(#{}, Ret01DestinationVariable)),
      ?assertMatch(
        {Ret21Result, _},
        wms_engine_lang_adapter:evaluate_variable(#{}, Ret21DestinationVariable)),
      ?assertMatch(
        {Ret22Result, _},
        wms_engine_lang_adapter:evaluate_variable(#{}, Ret22DestinationVariable)),
      ?assertMatch(
        {Ret30Result, _},
        wms_engine_lang_adapter:evaluate_variable(#{}, Ret30DestinationVariable))
    end,
  execute(Test,
          [{Var1, Var1Value}, {Var2, Var2Value}],
          [
            {<<"int01">>, #{<<"ret01">> => Ret01Result}},
            {<<"int21">>, #{<<"ret21">> => Ret21Result}},
            {<<"int22">>, #{<<"ret22">> => Ret22Result}},
            {<<"int30">>, #{<<"ret30">> => Ret30Result}}
          ]),


  % parallel execution error (int22)
  InteractionError = {error, no_service},

  Rules1 = [
             {rule, {
               [],
               [
                 {parallel, [
                   {call, {<<"int21">>,
                           [{<<"par21_01">>, Par2101Value}],
                           [{<<"ret21">>, Ret21DestinationVariable}]}},
                   {call, {<<"int22">>,
                           [{<<"par22_01">>, Par2201Value}],
                           [{<<"ret22">>, Ret22DestinationVariable}]}}
                 ]}
               ]
             }}
           ],
  Test1 =
    fun() ->
      {ok, Compiled} = wms_engine_precomp:compile(Rules1),
      AllID = wms_engine_precomp:get_ids(Compiled),
      State = #{impl => wms_engine_lang_adapter, executed => #{}},
      NewState =
        try
          wms_engine_lang:execute(Compiled, State),
          ?assert(false)
        catch
          throw:{parallel_errors, [{error, no_service}], St} ->
            St
        end,

      % execute with result true
      assert_exec_result(
        NewState, AllID,
        [<<"rule@1_le">>,
         <<"rule@1_parallel@1_call@1">>], true),

      % executed with result false
      assert_exec_result(
        NewState, AllID,
        [], false),

      % not executed
      assert_exec_result(
        NewState, AllID,
        [<<"rule@1">>, <<"rule@1_parallel@1">>,
         <<"rule@1_parallel@1_call@2">>], undefined),

      % check interaction
      ExpectedHistory =
        [
          {call,
           <<"int21">>,
           [{<<"par21_01">>, Par2101Value}]},
          {call,
           <<"int22">>,
           [{<<"par22_01">>, Par2201Value}]}
        ],
      [Seq | Parallels] = wms_engine_lang_adapter:get_history(),
      ?assertEqual(ExpectedHistory, [Seq | lists:sort(Parallels)]),

      % check interaction return value
      ?assertMatch(
        {Ret21Result, _},
        wms_engine_lang_adapter:evaluate_variable(#{}, Ret21DestinationVariable)),
      NewState
    end,
  LastState =
    execute(Test1,
            [{Var1, Var1Value}, {Var2, Var2Value}],
            [
              {<<"int21">>, #{<<"ret21">> => Ret21Result}},
              {<<"int22">>, InteractionError}
            ]),

  % int22 return value repaired, and runs again

  Test2 =
    fun() ->
      {ok, Compiled} = wms_engine_precomp:compile(Rules1),
      AllID = wms_engine_precomp:get_ids(Compiled),
      State = LastState,
      {ok, NewState} = wms_engine_lang:execute(Compiled, State),

      % execute with result true
      assert_exec_result(
        NewState, AllID,
        [<<"rule@1">>, <<"rule@1_le">>,
         <<"rule@1_parallel@1">>,
         <<"rule@1_parallel@1_call@1">>, <<"rule@1_parallel@1_call@2">>], true),

      % executed with result false
      assert_exec_result(
        NewState, AllID,
        [], false),

      % not executed
      assert_exec_result(
        NewState, AllID,
        [], undefined),

      % check interaction
      ExpectedHistory =
        [
          {call,
           <<"int22">>,
           [{<<"par22_01">>, Par2201Value}]}
        ],
      [Seq | Parallels] = wms_engine_lang_adapter:get_history(),
      ?assertEqual(ExpectedHistory, [Seq | lists:sort(Parallels)]),

      % check interaction return value
      ?assertMatch(
        {Ret22Result, _},
        wms_engine_lang_adapter:evaluate_variable(#{}, Ret22DestinationVariable))
    end,
  execute(Test2,
          [{Var1, Var1Value}, {Var2, Var2Value}],
          [
            {<<"int21">>, #{<<"ret21">> => Ret21Result}},
            {<<"int22">>, #{<<"ret22">> => Ret22Result}}
          ]),

  % variable not found in return value

  Test3 =
    fun() ->
      {ok, Compiled} = wms_engine_precomp:compile(Rules1),
      AllID = wms_engine_precomp:get_ids(Compiled),
      State = #{impl => wms_engine_lang_adapter, executed => #{}},
      NewState =
        try
          wms_engine_lang:execute(Compiled, State),
          ?assert(false)
        catch
          throw:{parallel_errors, [{not_found, retval, <<"ret22">>, _}], St} ->
            St
        end,

      % execute with result true
      assert_exec_result(
        NewState, AllID,
        [<<"rule@1_le">>,
         <<"rule@1_parallel@1_call@1">>], true),

      % executed with result false
      assert_exec_result(
        NewState, AllID,
        [], false),

      % not executed
      assert_exec_result(
        NewState, AllID,
        [<<"rule@1">>, <<"rule@1_parallel@1">>,
         <<"rule@1_parallel@1_call@2">>], undefined),

      % check interaction
      ExpectedHistory =
        [
          {call,
           <<"int21">>,
           [{<<"par21_01">>, Par2101Value}]},
          {call,
           <<"int22">>,
           [{<<"par22_01">>, Par2201Value}]}
        ],
      [Seq | Parallels] = wms_engine_lang_adapter:get_history(),
      ?assertEqual(ExpectedHistory, [Seq | lists:sort(Parallels)]),

      % check interaction return value
      ?assertMatch(
        {Ret21Result, _},
        wms_engine_lang_adapter:evaluate_variable(#{}, Ret21DestinationVariable)),
      NewState
    end,
  LastState =
    execute(Test3,
            [{Var1, Var1Value}, {Var2, Var2Value}],
            [
              {<<"int21">>, #{<<"ret21">> => Ret21Result}},
              {<<"int22">>, #{<<"retxx">> => any}}
            ]).


execute_cmd_test() ->
  ErrorMessage = <<"error message">>,
  VarError = {private, <<"varmsg">>},

  % error test
  Rules = [
            {rule, {
              [],
              [
                {cmd, {error, VarError}}
              ]
            }}
          ],
  Test =
    fun() ->

      {ok, Compiled} = wms_engine_precomp:compile(Rules),
      AllID = wms_engine_precomp:get_ids(Compiled),
      State = #{impl => wms_engine_lang_adapter, executed => #{}},
      try
        wms_engine_lang:execute(Compiled, State),
        ?assert(false)
      catch
        throw: {exit, error, Message, NewState} ->
          ?assertEqual(ErrorMessage, Message),
          assert_exec_result(
            NewState, AllID, [<<"rule@1_le">>], true),
          assert_exec_result(
            NewState, AllID, [<<"rule@1">>, <<"rule@1_cmd@1">>], undefined)
      end
    end,
  execute(Test,
          [{VarError, ErrorMessage}],
          []),

  Ret01Result = <<"this is the result-01">>,
  Ret01DestinationVariable = {private, <<"result01">>},

  % exit test
  Rules1 = [
             {rule, {
               [],
               [
                 {cmd, {exit, VarError}},
                 {call, {<<"int01">>,
                         [],
                         [{<<"ret01">>, Ret01DestinationVariable}]}
                 }
               ]
             }}
           ],
  Test1 =
    fun() ->

      {ok, Compiled} = wms_engine_precomp:compile(Rules1),
      AllID = wms_engine_precomp:get_ids(Compiled),
      State = #{impl => wms_engine_lang_adapter, executed => #{}},
      try
        wms_engine_lang:execute(Compiled, State),
        ?assert(false)
      catch
        throw: {exit, ok, Message, NewState} ->
          ?assertEqual(ErrorMessage, Message),
          assert_exec_result(
            NewState, AllID, [<<"rule@1_le">>], true),
          assert_exec_result(
            NewState, AllID,
            [<<"rule@1">>, <<"rule@1_cmd@1">>,
             <<"rule@1_call@1">>], undefined)
      end
    end,
  execute(Test1,
          [{VarError, ErrorMessage}],
          [{<<"int01">>, #{<<"ret01">> => Ret01Result}}]),

  % fire and wait test
  Rules2 = [
             {rule, {
               [],
               [
                 {cmd, {fire, <<"event01">>}},
                 {cmd, {wait, all, [<<"event01">>]}}
               ]
             }}
           ],
  Test3 =
    fun() ->
      {ok, Compiled} = wms_engine_precomp:compile(Rules2),
      AllID = wms_engine_precomp:get_ids(Compiled),
      State = #{impl => wms_engine_lang_adapter, executed => #{}},
      {ok, NewState} = wms_engine_lang:execute(Compiled, State),
      assert_exec_result(
        NewState, AllID,
        [<<"rule@1">>, <<"rule@1_le">>,
         <<"rule@1_cmd@1">>, <<"rule@1_cmd@2">>], true),

      % check commands
      ExpectedHistory =
        [
          {fire, <<"event01">>},
          {wait, all, [<<"event01">>]}
        ],
      Hist = wms_engine_lang_adapter:get_history(),
      ?assertEqual(ExpectedHistory, lists:sort(Hist))

    end,
  execute(Test3, [], []).

eval_comp_test() ->
  % string
  VarS1 = {private, <<"VarS1">>},
  ValS1 = "string1",
  VarS2 = {global, <<"VarS2">>},
  ValS2 = "string2",

  % integer
  VarI1 = {private, <<"VarI1">>},
  ValI1 = 1,
  VarI2 = {private, <<"VarI2">>},
  ValI2 = 2,

  % float
  VarF1 = {private, <<"VarF1">>},
  ValF1 = 1.1,
  VarF2 = {private, <<"VarF2">>},
  ValF2 = 2.2,

  % date/time
  VarD1 = {private, <<"VarD1">>},
  ValD1 = {1997, 9, 1},
  VarD2 = {private, <<"VarD2">>},
  ValD2 = {1997, 10, 1},

  % datetime
  VarDT1 = {private, <<"VarDT1">>},
  ValDT1 = {{1997, 9, 1}, {10, 1, 1}},
  VarDT2 = {private, <<"VarDT2">>},
  ValDT2 = {{1997, 9, 1}, {10, 1, 2}},

  % boolean
  VarB1 = {private, <<"VarB1">>},
  ValB1 = false,
  VarB2 = {global, <<"VarB2">>},
  ValB2 = true,

  State = #{impl => wms_engine_lang_adapter, executed => #{}},
  TestFun =
    fun() ->
      test_comparsion(VarS1, VarS2, ValS1, ValS2, State),
      test_comparsion(VarI1, VarI2, ValI1, ValI2, State),
      test_comparsion(VarF1, VarF2, ValF1, ValF2, State),
      test_comparsion(VarD1, VarD2, ValD1, ValD2, State),
      test_comparsion(VarDT1, VarDT2, ValDT1, ValDT2, State),
      test_comparsion(VarB1, VarB2, ValB1, ValB2, State),
      ?assertException(throw, {invalid, eval_co, '*', State},
                       wms_engine_lang:eval_ce({VarS1, '*', VarS1}, State))
    end,
  execute(TestFun, [
    {VarS1, ValS1}, {VarS2, ValS2},
    {VarI1, ValI1}, {VarI2, ValI2},
    {VarF1, ValF1}, {VarF2, ValF2},
    {VarD1, ValD1}, {VarD2, ValD2},
    {VarDT1, ValDT1}, {VarDT2, ValDT2},
    {VarB1, ValB1}, {VarB2, ValB2}
  ],      []).

test_comparsion(VarS1, VarS2, ValS1, ValS2, State) ->
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '=', VarS1}, State)),
  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '=', VarS2}, State)),
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '=', ValS1}, State)),
  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '=', ValS2}, State)),

  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '>', VarS1}, State)),
  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '>', VarS2}, State)),
  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '>', ValS1}, State)),
  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '>', ValS2}, State)),

  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '>=', VarS1}, State)),
  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '>=', VarS2}, State)),
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '>=', ValS1}, State)),
  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '>=', ValS2}, State)),

  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '<', VarS1}, State)),
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '<', VarS2}, State)),
  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '<', ValS1}, State)),
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '<', ValS2}, State)),

  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '<=', VarS1}, State)),
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '<=', VarS2}, State)),
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '<=', ValS1}, State)),
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '<=', ValS2}, State)),

  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '!=', VarS1}, State)),
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '!=', VarS2}, State)),
  ?assertMatch({false, _}, wms_engine_lang:eval_ce({VarS1, '!=', ValS1}, State)),
  ?assertMatch({true, _}, wms_engine_lang:eval_ce({VarS1, '!=', ValS2}, State)).


eval_boolean_test() ->
  State = #{impl => wms_engine_lang_adapter, executed => #{}},
  TestFun =
    fun() ->
      ?assertMatch({true, _}, wms_engine_lang:eval_bo('and', true, true, State)),
      ?assertMatch({false, _}, wms_engine_lang:eval_bo('and', false, true, State)),
      ?assertMatch({false, _}, wms_engine_lang:eval_bo('and', true, false, State)),
      ?assertMatch({false, _}, wms_engine_lang:eval_bo('and', false, false, State)),

      ?assertMatch({true, _}, wms_engine_lang:eval_bo('or', true, true, State)),
      ?assertMatch({true, _}, wms_engine_lang:eval_bo('or', false, true, State)),
      ?assertMatch({true, _}, wms_engine_lang:eval_bo('or', true, false, State)),
      ?assertMatch({false, _}, wms_engine_lang:eval_bo('or', false, false, State)),

      ?assertMatch({false, _}, wms_engine_lang:eval_bo('xor', true, true, State)),
      ?assertMatch({true, _}, wms_engine_lang:eval_bo('xor', false, true, State)),
      ?assertMatch({true, _}, wms_engine_lang:eval_bo('xor', true, false, State)),
      ?assertMatch({false, _}, wms_engine_lang:eval_bo('xor', false, false, State)),

      ?assertMatch({true, _},
                   wms_engine_lang:eval_bo('set', true, undefined, State)),
      ?assertMatch({false, _},
                   wms_engine_lang:eval_bo('set', false, undefined, State)),

      ?assertException(throw, {invalid, bool_op, bad, State},
                       wms_engine_lang:eval_bo(bad, false, true, State))
    end,
  execute(TestFun, [], []).

%% =============================================================================
%% Private functions
%% =============================================================================

execute(TestFun, InitVars, InteractionReplies) ->
  try
    wms_engine_lang_adapter:init(),

    lists:foreach(
      fun({Var, Value}) ->
        wms_engine_lang_adapter:set(Var, Value)
      end, InitVars),

    lists:foreach(
      fun({ID, Retvals}) ->
        wms_engine_lang_adapter:set(ID, Retvals)
      end, InteractionReplies),

    TestFun()
  after
    destroy_adapter()
  end.

destroy_adapter() ->
  try
    wms_engine_lang_adapter:destroy()
  catch
    _:_ ->
      ok
  end.

assert_exec_result(#{executed := Executed}, AllID, ExpectedIDS, ExecResult) ->
  lists:foreach(
    fun(ID) ->
      ?assertEqual(true, lists:member(ID, AllID), <<"Missing: ", ID/binary>>),
      Default = case ExecResult of
                  undefined ->
                    undefined;
                  Logical ->
                    not Logical
                end,
      ?assertEqual(ExecResult, maps:get(ID, Executed, Default), ID)
    end, ExpectedIDS
  ).