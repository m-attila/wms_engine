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
  Ret01DestinationVariable = {private, <<"result01">>},
  Ret21DestinationVariable = {private, <<"result21">>},
  Ret22DestinationVariable = {private, <<"result22">>},

  Rules = [
            {rule, {
              [
                {'or', {Var1, '>=', 1}},
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
                ]}
              ]
            }}
          ],

  Test =
    fun() ->
      {ok, Compiled} = wms_engine_precomp:compile(Rules),
      State = #{impl => wms_engine_lang_adapter, executed => #{}},
      {ok, _} = wms_engine_lang:execute(Compiled, State),

      % check interaction
      ExpectedHistory =
        [
          {call,
           <<"int01">>,
           [{<<"par01_01">>, Par0101Value}, {<<"par01_02">>, Var2Value}]},
          {parallel, [
            {call,
             <<"int21">>,
             [{<<"par21_01">>, Par2101Value}]},
            {call,
             <<"int22">>,
             [{<<"par22_01">>, Par2201Value}]}
          ]}
        ],
      ?assertEqual(ExpectedHistory, wms_engine_lang_adapter:get_history()),

      % check interaction return value
      ?assertMatch(
        {Ret01Result, _},
        wms_engine_lang_adapter:evaluate_variable(#{}, Ret01DestinationVariable))

    end,
  execute(Test,
          [{Var1, Var1Value}, {Var2, Var2Value}],
          [
            {<<"int01">>, #{<<"ret01">> => Ret01Result}},
            {<<"int21">>, #{<<"ret21">> => Ret21Result}},
            {<<"int22">>, #{<<"ret22">> => Ret22Result}}
          ]).


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
    try
      wms_engine_lang_adapter:destroy()
    catch
      _:_ ->
        ok
    end
  end.