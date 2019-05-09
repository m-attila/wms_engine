%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%%
%%% @end
%%% Created : 03. May 2019 19:13
%%%-------------------------------------------------------------------
-author("Attila Makra").
-include_lib("wms_state/include/wms_state.hrl").

%% -----------------------------------------------------------------------------
%% Execution states
%% -----------------------------------------------------------------------------
-type execution_result() :: term().

-type engine_state() :: #{
impl := atom(),
executed := #{integer() := execution_result()}
}.

-type step_state() :: UniqueID :: binary().

%% -----------------------------------------------------------------------------
%% Base types.
%% -----------------------------------------------------------------------------

-type event_id() :: identifier_name().

%% -----------------------------------------------------------------------------
%% Operations
%% -----------------------------------------------------------------------------

-type no_op() :: nop.
% number operations
-type number_base_op() :: {'+' | '-' | '*' | '/', variable_or_literal()}.

% string operations
-type string_concat() :: {'+', variable_or_literal()}.

% logical operations
-type logical_or() :: {'+', variable_or_literal()}.
-type logical_and() :: {'*', variable_or_literal()}.
-type logical_xor() :: {'-', variable_or_literal()}.
-type logical_not() :: '!'.
-type logical_operations() :: logical_or() | logical_and() | logical_xor() |
logical_not().

% date operations
-type date_mod() :: {{'+' | '-', day | month | year}, variable_or_literal()}.
-type date_set() :: {{set, day | month | year}, variable_or_literal() | 'end'}.
-type date_operations() :: date_mod() | date_set().

% time operations
-type time_mod() :: {{'+' | '-', second | minute | hour}, variable_or_literal()}.
-type time_set() :: {{set, second | minute | hour}, variable_or_literal()}.
-type time_operations() :: time_mod() | time_set().

% list operations
-type list_add() :: {{'++', head | tail}, variable_or_literal()}.
-type list_rem() :: {{'--', head | tail}}.
-type list_get() :: {{'?', head | tail}}.
-type list_union() :: {'+', variable_or_literal()}.
-type list_inters() :: {'*', variable_or_literal()}.
-type list_sub() :: {'-', variable_or_literal()}.
-type list_operations() :: list_add() | list_rem() | list_get() | list_union() |
list_inters() | list_sub().

-type operation() :: number_base_op() | string_concat() | logical_operations() |
date_operations() | time_operations() | list_operations() | no_op().

%% -----------------------------------------------------------------------------
%% Commands
%% -----------------------------------------------------------------------------
-type wait_type() :: all | any.
-type command() ::
{cmd, {error, literal(string()) | variable_reference()}}|
{cmd, {exit, literal(string()) | variable_reference()}}|
{cmd, {wait, wait_type(), [event_id()]}}|
{cmd, {fire, event_id()}}|
{cmd, {move, source(), destination()}}|
{cmd, {move, {source(), operation()}, destination()}}.

-type compiled_command() :: {step_state(), command()}.

%% -----------------------------------------------------------------------------
%% Interactions
%% -----------------------------------------------------------------------------

-type parameter_spec() :: {identifier_name(), variable_or_literal()}.
-type parameter_value() :: {identifier_name(), literal()}.

-type return_value_spec() :: {identifier_name(), variable_reference()}.
-type return_values() :: #{binary() => literal()}.
-type interaction() :: {call, {identifier_name(), [parameter_spec()],
                               [return_value_spec()]}}.
-type compiled_interaction() :: {step_state(), interaction()}.

-type parallel() :: {parallel, [interaction()]}.
-type compiled_parallel() :: {step_state(), {parallel, [compiled_interaction()]}}.

%% -----------------------------------------------------------------------------
%% Rules
%% -----------------------------------------------------------------------------

-type comparator_op() :: '=' | '>' | '<' | '>=' | '<=' | '!='.
-type comparator_expr() :: {variable_reference(),
                            comparator_op(), variable_or_literal()}.
-type bool_op() :: set | 'and' | 'or' | 'xor'.
-type logical_expr() :: [{bool_op(), comparator_expr()}].

-type step() :: command() | interaction() | parallel().
-type compiled_step() :: compiled_command() | compiled_interaction() | compiled_parallel().

-type rule() :: {rule, {logical_expr(), [step() | rule()]}}.
-type entry() :: rule() | step().

-type compiled_rule() :: {step_state(),
                          {rule, {logical_expr(), [compiled_step() | compiled_rule()]}}}.
-type compiled_entry() :: compiled_rule() | compiled_step().


