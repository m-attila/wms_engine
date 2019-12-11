%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, Attila Makra.
%%% @doc
%%% Unit test for wms_engine_precomt
%%% @end
%%% Created : 04. May 2019 10:49
%%%-------------------------------------------------------------------
-module(wms_engine_precomp_test).
-author("Attila Makra").

-include("../src/wms_engine_lang.hrl").
-include_lib("eunit/include/eunit.hrl").

precompile_test() ->
  % empty
  ?assertEqual({ok, []}, wms_engine_precomp:compile([])),

  NoStepRule = {rule, {logical_expr, []}},
  Expected = {<<"rule@1">>, NoStepRule},

  ?assertEqual({ok, [Expected]}, wms_engine_precomp:compile([NoStepRule])),

  % rules and steps
  Rule1 =
    {rule,
     {logical_expr1,
      [
        {rule,
         {logical_expr2, [
           {cmd, command2},
           {call, {a1, a2, a3}},
           {parallel, [
             {call, {a11, a12, a13}},
             {call, {a21, a22, a23}}
           ]}
         ]}
        },
        {cmd, command1}
      ]}
    },

  Rule2 = {rule,
           {logical_expr2, [
             {call, {a31, a32, a33}},
             {call, {a41, a42, a43}}
           ]}
          },

  Expected1 =
    [
      {<<"rule@1">>, {rule,
                      {logical_expr1, [
                        {<<"rule@1_rule@1">>,
                         {rule,
                          {logical_expr2, [
                            {<<"rule@1_rule@1_cmd@1">>,
                             {cmd, command2}},
                            {<<"rule@1_rule@1_call@1">>,
                             {call, {a1, a2, a3}}},
                            {<<"rule@1_rule@1_parallel@1">>,
                             {parallel, [
                               {<<"rule@1_rule@1_parallel@1_call@1">>,
                                {call, {a11, a12, a13}}},
                               {<<"rule@1_rule@1_parallel@1_call@2">>,
                                {call, {a21, a22, a23}}}
                             ]}}
                          ]}}
                        },
                        {<<"rule@1_cmd@1">>, {cmd, command1}}
                      ]}}
      },
      {<<"rule@2">>, {rule,
                      {logical_expr2, [
                        {<<"rule@2_call@1">>, {call, {a31, a32, a33}}},
                        {<<"rule@2_call@2">>, {call, {a41, a42, a43}}}
                      ]}}
      }
    ],

  Rules = [Rule1, Rule2],
  ?assertEqual({ok, Expected1}, wms_engine_precomp:compile(Rules)),

  ExpectedRules = lists:sort(
    [<<"rule@1">>, <<"rule@1_le">>,
     <<"rule@1_rule@1">>, <<"rule@1_rule@1_le">>,
     <<"rule@1_rule@1_cmd@1">>,
     <<"rule@1_rule@1_call@1">>, <<"rule@1_rule@1_parallel@1">>,
     <<"rule@1_rule@1_parallel@1_call@1">>, <<"rule@1_rule@1_parallel@1_call@2">>,
     <<"rule@1_cmd@1">>,
     <<"rule@2">>, <<"rule@2_le">>,
     <<"rule@2_call@1">>, <<"rule@2_call@2">>]),
  ?assertEqual(ExpectedRules,
               lists:sort(wms_engine_precomp:get_ids(Expected1))).
