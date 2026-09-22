"""Faithful port of the n8n v2 IF-node conditions (pure predicates, no I/O).

Source nodes (extracted/nodes/IF_*.json, typeVersion 2.2, options
typeValidation='strict', version=2, combinator='and' in every node):

  IF_Approved_Create_Action.json     -> if_approved_create_action
  IF_Approved_Cancel_Action.json     -> if_approved_cancel_action
  IF_Approved_Reschedule_Action.json -> if_approved_reschedule_action
  IF_Business_Time_Allowed.json      -> if_business_time_allowed
  IF_Operation_Claim_Required.json   -> if_operation_claim_required
  IF_Claim_Allows_Child.json         -> if_claim_allows_child
  IF_Contract_Needs_Repair.json      -> if_contract_needs_repair

n8n v2 operator semantics implemented here (from
n8n/packages/workflow/src/node-parameters/filter-parameter.ts):
  - boolean 'true'   -> `case 'true': return left;`  i.e. the parsed leftValue
    itself, so the condition is true iff leftValue === true.
  - string 'equals'  -> `left === right` (caseSensitive: true).
  - combinator 'and' -> all conditions must hold.
  - typeValidation 'strict': a leftValue of the WRONG type raises a node error
    in n8n; null/undefined pass validation and compare falsy. The upstream
    deterministic ports always emit real booleans/strings for these keys, so a
    non-boolean left value is treated as False here rather than raised
    (PORT-TODO(n8n) noted per predicate).
"""
from __future__ import annotations

from typing import Any, Dict
from app.core.js_semantics import string_equals as _string_equals

_UNDEFINED = object()


def _prop(item: Dict[str, Any], key: str) -> Any:
    if isinstance(item, dict):
        return item.get(key, _UNDEFINED)
    return _UNDEFINED


def _bool_true(value: Any) -> bool:
    """n8n v2 boolean operator 'true' under strict validation: left === True.

    undefined/null compare falsy; a non-boolean left value raises in n8n strict
    mode and is treated as False here (upstream ports guarantee booleans).
    """
    return value is True


def _decision_allowed_value(item: Dict[str, Any]) -> Any:
    """leftValue of the Approved-*-Action allowed condition:

    $json.system_decision && $json.system_decision.allowed !== undefined
      ? $json.system_decision.allowed
      : ($json.allowed !== undefined ? $json.allowed : false)
    (JS precedence: (a && b) ? x : y.)
    """
    system_decision = _prop(item, "system_decision")
    if system_decision is not None and system_decision is not _UNDEFINED and _prop(system_decision, "allowed") is not _UNDEFINED:
        return _prop(system_decision, "allowed")
    if _prop(item, "allowed") is not _UNDEFINED:
        return _prop(item, "allowed")
    return False


def _decision_action_value(item: Dict[str, Any]) -> Any:
    """leftValue of the Approved-*-Action action condition:

    $json.system_decision && $json.system_decision.action !== undefined
      ? $json.system_decision.action
      : ($json.action !== undefined ? $json.action : "")
    """
    system_decision = _prop(item, "system_decision")
    if system_decision is not None and system_decision is not _UNDEFINED and _prop(system_decision, "action") is not _UNDEFINED:
        return _prop(system_decision, "action")
    if _prop(item, "action") is not _UNDEFINED:
        return _prop(item, "action")
    return ""


def if_approved_create_action(item: Dict[str, Any]) -> bool:
    """Source node: IF Approved Create Action (extracted/nodes/IF_Approved_Create_Action.json).

    combinator 'and':
      1. allowed flag is true (see _decision_allowed_value)
      2. action equals 'create_appointment'
      3. orchestrator decision has patient_data_complete === true and
         patient_data_gate_satisfied === true.
         PORT-TODO(n8n): the third condition reads
         $('System Orchestrator (Policy)').first().json directly; the port reads
         item.system_decision — the pipeline runner must guarantee the item
         carries the orchestrator's decision (the gate spreads preserve it).
    """
    allowed = _decision_allowed_value(item)
    action = _decision_action_value(item)
    decision = _prop(item, "system_decision")
    decision = decision if isinstance(decision, dict) else {}
    cond1 = _bool_true(allowed)
    cond2 = _string_equals(action, "create_appointment")
    cond3 = _prop(decision, "patient_data_complete") is True and _prop(decision, "patient_data_gate_satisfied") is True
    return cond1 and cond2 and cond3


def if_approved_cancel_action(item: Dict[str, Any]) -> bool:
    """Source node: IF Approved Cancel Action (extracted/nodes/IF_Approved_Cancel_Action.json).

    combinator 'and': allowed flag is true AND action equals 'cancel_appointment'.
    """
    allowed = _decision_allowed_value(item)
    action = _decision_action_value(item)
    cond1 = _bool_true(allowed)
    cond2 = _string_equals(action, "cancel_appointment")
    return cond1 and cond2


def if_approved_reschedule_action(item: Dict[str, Any]) -> bool:
    """Source node: IF Approved Reschedule Action (extracted/nodes/IF_Approved_Reschedule_Action.json).

    combinator 'and': allowed flag is true AND action equals
    'reschedule_appointment'.
    """
    allowed = _decision_allowed_value(item)
    action = _decision_action_value(item)
    cond1 = _bool_true(allowed)
    cond2 = _string_equals(action, "reschedule_appointment")
    return cond1 and cond2


def if_business_time_allowed(item: Dict[str, Any]) -> bool:
    """Source node: IF Business Time Allowed (extracted/nodes/IF_Business_Time_Allowed.json).

    combinator 'and': boolean 'true' on $json.business_time_allowed.
    """
    return _bool_true(_prop(item, "business_time_allowed"))


def if_operation_claim_required(item: Dict[str, Any]) -> bool:
    """Source node: IF Operation Claim Required (extracted/nodes/IF_Operation_Claim_Required.json).

    combinator 'and': boolean 'true' on $json.claim_required.
    """
    return _bool_true(_prop(item, "claim_required"))


def if_claim_allows_child(item: Dict[str, Any]) -> bool:
    """Source node: IF Claim Allows Child (extracted/nodes/IF_Claim_Allows_Child.json).

    combinator 'and': boolean 'true' on $json.child_execution_allowed.
    """
    return _bool_true(_prop(item, "child_execution_allowed"))


def if_contract_needs_repair(item: Dict[str, Any]) -> bool:
    """Source node: IF Contract Needs Repair (extracted/nodes/IF_Contract_Needs_Repair.json).

    combinator 'and': leftValue expression `$json._contract_repair_needed === true`
    fed to the boolean 'true' operator — the double-true collapses to
    item._contract_repair_needed is True.
    """
    left_value = _prop(item, "_contract_repair_needed") is True
    return _bool_true(left_value)
