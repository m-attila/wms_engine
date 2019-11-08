%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%% Testcases for wms_engine
%%% @end
%%% Created : 09. May 2019 22:19
%%%-------------------------------------------------------------------
-module(wms_engine_SUITE).
-author("Attila Makra").

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("wms_test/include/wms_test.hrl").
-include("wms_engine.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Returns list of tuples to set default properties
%%              for the suite.
%%
%% Note: The suite/0 function is only meant to be used to return
%% default data values, not perform any other operations.
%%--------------------------------------------------------------------
suite() ->
  [{key, value}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the suite.
%%
%% Description: Initialization before the suite.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_suite(Config) ->
  application:ensure_all_started(?APP_NAME),
  [{key, value} | Config].

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> term() | {save_config,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after the suite.
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
  ok.

%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%% Reason = term()
%%   The reason for skipping all test cases and subgroups in the group.
%%
%% Description: Initialization before each test case group.
%%--------------------------------------------------------------------
init_per_group(GroupName, Config) ->
  ?MODULE:GroupName({prelude, Config}).

%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               term() | {save_config,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%%
%% Description: Cleanup after each test case group.
%%--------------------------------------------------------------------
end_per_group(GroupName, Config) ->
  ?MODULE:GroupName({postlude, Config}).

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% TestCase = atom()
%%   Name of the test case that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%%
%% Description: Initialization before each test case.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
  ?MODULE:TestCase({prelude, Config}).

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config,Config1} | {fail,Reason}
%%
%% TestCase = atom()
%%   Name of the test case that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for failing the test case.
%%
%% Description: Cleanup after each test case.
%%--------------------------------------------------------------------
end_per_testcase(TestCase, Config) ->
  ?MODULE:TestCase({postlude, Config}).

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%%
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%%   The name of the group.
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%%   Group properties that may be combined.
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%%   The name of a test case.
%% Shuffle = shuffle | {shuffle,Seed}
%%   To get cases executed in random order.
%% Seed = {integer(),integer(),integer()}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%%   To get execution of cases repeated.
%% N = integer() | forever
%%
%% Description: Returns a list of test case group definitions.
%%--------------------------------------------------------------------
groups() ->
  [
    {db_state_group,
     [{repeat_until_any_fail, 1}],
     [
       private_var_test,
       global_var_test,
       transaction_var_test
     ]
    },
    {engine_actor_group,
     [{repeat_until_any_fail, 1}],
     [
       simple_task_test,
       broken_task_test,
       manual_task_start_test,
       event_handling_task_test
     ]
    }
  ].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%%
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%%   Name of a test case group.
%% TestCase = atom()
%%   Name of a test case.
%% Reason = term()
%%   The reason for skipping all groups and test cases.
%%
%% Description: Returns the list of groups and test cases that
%%              are to be executed.
%%--------------------------------------------------------------------
all() ->
  [
    {group, db_state_group},
    {group, engine_actor_group}
  ].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%% Comment = term()
%%   A comment about the test case that will be printed in the html log.
%%
%% Description: Test case function. (The name of it must be specified in
%%              the all/0 list or in a test case group for the test case
%%              to be executed).
%%--------------------------------------------------------------------

%% =============================================================================
%% group
%% =============================================================================

db_state_group({prelude, Config}) ->
  {ok, StartedApps} = application:ensure_all_started(?APP_NAME),
  ?assertEqual(ok, wms_db_handler_service:wait_for_initialized(3000)),
  [{started, StartedApps} | Config];
db_state_group({postlude, Config}) ->
  clear_tables(),
  StartedApps = ?config(started, Config),
  [application:stop(App) || App <- StartedApps].

%%--------------------------------------------------------------------
%% Private variable tests
%%
%%--------------------------------------------------------------------

%% test case information
private_var_test({info, _Config}) ->
  [""];
private_var_test(suite) ->
  ok;
%% init test case
private_var_test({prelude, Config}) ->
  Config;
%% destroy test case
private_var_test({postlude, _Config}) ->
  ok;
%% test case implementation
private_var_test(_Config) ->
  VariableIDFnd = <<"var1">>,
  Value = 123,
  VariableIDNotFnd = <<"notfoundvar">>,

  Env = #{private => #{
    VariableIDFnd => Value
  }},

  ?assertEqual({error,
                {not_found, variable, VariableIDNotFnd, Env}},
               wms_engine_db_state:get_variable(Env, {private, VariableIDNotFnd})),
  ?assertEqual({ok, Value},
               wms_engine_db_state:get_variable(Env, {private, VariableIDFnd})),

  Value2 = "apple",
  {ok, Env2} = wms_engine_db_state:set_variable(Env,
                                                {private, VariableIDNotFnd},
                                                Value2, true),
  ?assertEqual({ok, Value2},
               wms_engine_db_state:get_variable(Env2, {private, VariableIDNotFnd})).

%%--------------------------------------------------------------------
%% Global variables test
%%
%%--------------------------------------------------------------------

%% test case information
global_var_test({info, _Config}) ->
  [""];
global_var_test(suite) ->
  ok;
%% init test case
global_var_test({prelude, Config}) ->
  Config;
%% destroy test case
global_var_test({postlude, _Config}) ->
  ok;
%% test case implementation
global_var_test(_Config) ->
  Value = 123,
  VariableID = <<"var1">>,

  Env = #{},

  ?assertEqual({error,
                {not_found, variable, VariableID, Env}},
               wms_engine_db_state:get_variable(Env, {global, VariableID})),

  {ok, Env2} = wms_engine_db_state:set_variable(Env,
                                                {global, VariableID},
                                                Value, true),
  ?assertEqual({ok, Value},
               wms_engine_db_state:get_variable(Env2, {global, VariableID})).

%%--------------------------------------------------------------------
%% Test for transactional modification
%%
%%--------------------------------------------------------------------

%% test case information
transaction_var_test({info, _Config}) ->
  [""];
transaction_var_test(suite) ->
  ok;
%% init test case
transaction_var_test({prelude, Config}) ->
  Config;
%% destroy test case
transaction_var_test({postlude, _Config}) ->
  ok;
%% test case implementation
transaction_var_test(_Config) ->
  ValueP = 123,
  ValueG = 456,

  VariablePID = <<"var1">>,
  VariableGID = <<"var2">>,

  Env = #{private => #{}},

  % success

  Transaction =
    fun(Environment) ->
      {ok, Env1} = wms_engine_db_state:set_variable(Environment,
                                                    {private, VariablePID},
                                                    ValueP, true),
      wms_engine_db_state:set_variable(Env1,
                                       {global, VariableGID},
                                       ValueG, true)
    end,
  {ok, Env3} = wms_engine_db_state:transaction(Env, Transaction),

  ?assertEqual({ok, ValueP},
               wms_engine_db_state:get_variable(Env3, {private, VariablePID})),
  ?assertEqual({ok, ValueG},
               wms_engine_db_state:get_variable(Env3, {global, VariableGID})),
  ok,

  % transaction aborted
  Transaction2 =
    fun(Environment) ->
      {ok, Env1} = wms_engine_db_state:set_variable(Environment,
                                                    {private, VariablePID},
                                                    ValueP + 100, true),
      wms_engine_db_state:set_variable(Env1,
                                       {global, VariableGID},
                                       ValueG + 100, true),
      throw(any_error)
    end,
  {error, any_error} = wms_engine_db_state:transaction(Env3, Transaction2),

  ?assertEqual({ok, ValueP},
               wms_engine_db_state:get_variable(Env3, {private, VariablePID})),
  ?assertEqual({ok, ValueG},
               wms_engine_db_state:get_variable(Env3, {global, VariableGID})).

%% =============================================================================
%% Group
%% =============================================================================

engine_actor_group({prelude, Config}) ->
  {ok, StartedApps} = application:ensure_all_started(?APP_NAME),
  ?assertEqual(ok, wms_db_handler_service:wait_for_initialized(3000)),
  [{started, StartedApps} | Config];
engine_actor_group({postlude, Config}) ->
  clear_tables(),
  StartedApps = ?config(started, Config),
  [application:stop(App) || App <- StartedApps].

%%--------------------------------------------------------------------
%% Execute simple task
%%
%%--------------------------------------------------------------------

%% test case information
simple_task_test({info, _Config}) ->
  [""];
simple_task_test(suite) ->
  ok;
%% init test case
simple_task_test({prelude, Config}) ->
  clear_tables(),
  TaskName = <<"test_task">>,
  Var1 = {private, <<"var1">>},
  Var2 = {private, <<"var2">>},
  Var3 = {global, <<"var3">>},

  TaskRules = [
                {rule,
                 {
                   [],
                   [
                     {cmd, {move, 10, Var1}},
                     {cmd, {move, 20, Var2}},
                     {cmd, {move, {Var1, {'+', Var2}}, Var3}}
                   ]
                 }
                }
              ],
  wms_db:add_taskdef(TaskName, TaskRules, auto),
  [{task_name, TaskName},
   {result_var, Var3} | Config];
%% destroy test case
simple_task_test({postlude, Config}) ->
  TaskName = ?config(task_name, Config),
  wms_db:remove_taskdef(TaskName),
  ok;
%% test case implementation
simple_task_test(Config) ->
  {_, ResultVar} = ?config(result_var, Config),
  wms_dist:call(wms_engine_actor, restart_task_controller, []),

  % check global result in global variable "var3"
  wait_for(
    fun() ->
      case mnesia:dirty_read(global_state, ResultVar) of
        [] ->
          false;
        [{_, ResultVar, 30}] ->
          true
      end
    end, 2000).

%%--------------------------------------------------------------------
%% Execute task which will be aborted, then restart
%%
%%--------------------------------------------------------------------

%% test case information
broken_task_test({info, _Config}) ->
  [""];
broken_task_test(suite) ->
  ok;
%% init test case
broken_task_test({prelude, Config}) ->
  clear_tables(),
  TaskName = <<"test_task">>,
  Var1 = {private, <<"var1">>},
  Var2 = {global, <<"var2">>},
  Var3 = {global, <<"var3">>},

  TaskRules = [
                {rule,
                 {
                   [],
                   [
                     {cmd, {move, 10, Var1}},
                     % in first run, var2 has no value
                     {cmd, {move, {Var1, {'+', Var2}}, Var3}}
                   ]
                 }
                }
              ],
  wms_db:add_taskdef(TaskName, TaskRules, auto),
  [{task_name, TaskName},
   {missing_var, Var2},
   {result_var, Var3} | Config];
%% destroy test case
broken_task_test({postlude, Config}) ->
  TaskName = ?config(task_name, Config),
  wms_db:remove_taskdef(TaskName),
  ok;
%% test case implementation
broken_task_test(Config) ->
  {_, ResultVar} = ?config(result_var, Config),
  {_, MissingVar} = ?config(missing_var, Config),

  % task will be failed, because Var2 is not defined
  wms_dist:call(wms_engine_actor, restart_task_controller, []),

  timer:sleep(2000),

  % set missing variable
  wms_engine_db_state:set_variable(#{}, {global, MissingVar}, 40, undefined),

  % start task again
  wms_dist:call(wms_engine_actor, restart_task_controller, []),


  % check global result in global variable "var3"
  wait_for(
    fun() ->
      case mnesia:dirty_read(global_state, ResultVar) of
        [] ->
          false;
        [{_, ResultVar, 50}] ->
          true
      end
    end, 2000).

%%--------------------------------------------------------------------
%% Manually task start tests
%%
%%--------------------------------------------------------------------

%% test case information
manual_task_start_test({info, _Config}) ->
  [""];
manual_task_start_test(suite) ->
  ok;
%% init test case
manual_task_start_test({prelude, Config}) ->
  clear_tables(),
  TaskName = <<"test_task">>,
  Var1 = {private, <<"var1">>},
  Var2 = {private, <<"var2">>},
  Var3 = {global, <<"var3">>},

  TaskRules = [
                {rule,
                 {
                   [],
                   [
                     {cmd, {move, 10, Var1}},
                     {cmd, {move, 20, Var2}},
                     {cmd, {move, {Var1, {'+', Var2}}, Var3}}
                   ]
                 }
                }
              ],
  wms_db:add_taskdef(TaskName, TaskRules, auto),
  [{task_name, TaskName},
   {result_var, Var3} | Config];
%% destroy test case
manual_task_start_test({postlude, Config}) ->
  TaskName = ?config(task_name, Config),
  wms_db:remove_taskdef(TaskName),
  ok;
%% test case implementation
manual_task_start_test(Config) ->
  {_, ResultVar} = ?config(result_var, Config),
  TaskName = ?config(task_name, Config),

  % invalid task name
  {error, not_found} = wms_dist:call(wms_engine_actor, start_task, [<<>>]),

  ok = wms_dist:call(wms_engine_actor, start_task, [TaskName]),

  % check global result in global variable "var3"
  wait_for(
    fun() ->
      case mnesia:dirty_read(global_state, ResultVar) of
        [] ->
          false;
        [{_, ResultVar, 30}] ->
          true
      end
    end, 2000).

%%--------------------------------------------------------------------
%% Task waiting for event test
%%
%%--------------------------------------------------------------------

%% test case information
event_handling_task_test({info, _Config}) ->
  [""];
event_handling_task_test(suite) ->
  ok;
%% init test case
event_handling_task_test({prelude, Config}) ->
  clear_tables(),

  % task1
  TaskName1 = <<"task1">>,
  TaskRules1 = [
                 {rule, {
                   [],
                   [
                     {cmd, {wait, all, [<<"task2_var_set">>]}},
                     {cmd, {move,
                            {{global, <<"task2_var">>}, {'+', 30}},
                            {global, <<"task1_var">>}}
                     },
                     {cmd, {fire, {mandatory, <<"task1_var_set">>}}}
                   ]
                 }}
               ],
  wms_db:add_taskdef(TaskName1, TaskRules1, auto),

  % task2
  TaskName2 = <<"task2">>,
  TaskRules2 = [
                 {rule, {
                   [],
                   [
                     {cmd, {move, 20, {global, <<"task2_var">>}}},
                     {cmd, {fire, {mandatory, <<"task2_var_set">>}}},
                     {cmd, {wait, any, [<<"task1_var_set">>]}},
                     {cmd, {move,
                            {{global, <<"task1_var">>}, {'+', 40}},
                            {global, <<"task3_var">>}}
                     }
                   ]}
                 }
               ],
  wms_db:add_taskdef(TaskName2, TaskRules2, auto),


  Config;
%% destroy test case
event_handling_task_test({postlude, _Config}) ->
  ok;
%% test case implementation
event_handling_task_test(_Config) ->
  wms_engine_task_controller:manual_start_task(<<"task1">>),
  wms_engine_task_controller:manual_start_task(<<"task2">>),

  wait_for(
    fun() ->
      case mnesia:dirty_read(global_state, <<"task3_var">>) of
        [] ->
          false;
        [{_, <<"task3_var">>, 90}] ->
          true
      end
    end, 2000).


%% =============================================================================
%% Private test functions
%% =============================================================================

% Remove db tables from mnesia
delete_tables() ->
  [{atomic, ok} = mnesia:delete_table(Table) ||
    Table <- mnesia:system_info(tables),
   Table =/= schema].

% Clear db tables from mnesia
clear_tables() ->
  [{atomic, ok} = mnesia:clear_table(Table) ||
    Table <- mnesia:system_info(tables),
   Table =/= schema].

wait_for(_, Timeout) when Timeout =< 0 ->
  throw(timeout);
wait_for(Condition, Timeout) ->
  case Condition() of
    true ->
      ok;
    false ->
      timer:sleep(100),
      wait_for(Condition, Timeout - 100)
  end.