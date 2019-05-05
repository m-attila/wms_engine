%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
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
  Expected = {2, NoStepRule},

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
      {10, {rule,
           {logical_expr1, [
             {7, {rule,
                  {logical_expr2, [
                    {1, {cmd, command2}},
                    {2, {call, {a1, a2, a3}}},
                    {5, {parallel, [
                      {3, {call, {a11, a12, a13}}},
                      {4, {call, {a21, a22, a23}}}
                    ]}}
                  ]}}
             },
             {8, {cmd, command1}}
           ]}}
      },
      {14, {rule,
            {logical_expr2, [
              {11, {call, {a31, a32, a33}}},
              {12, {call, {a41, a42, a43}}}
            ]}}
      }
    ],

  Rules = [Rule1, Rule2],
  ?assertEqual({ok, Expected1}, wms_engine_precomp:compile(Rules)).
