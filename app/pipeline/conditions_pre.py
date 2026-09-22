"""Faithful port of the pre-agent n8n v2 IF-node conditions (pure predicates, no I/O).

Source nodes (extracted/nodes/, typeVersion 2.2, combinator 'and' in every node):

  Check_Duplicate_Message.json       -> if_check_duplicate_message
  Check_Patient_Ownership.json       -> if_check_patient_ownership
  IF_Early_Security_Reject.json      -> if_early_security_reject
  IF_K2_Signature_Accepted.json      -> if_k2_signature_accepted
  IF_K2_Burst_Allowed.json           -> if_k2_burst_allowed
  Completed_Create_Replay.json       -> if_completed_create_replay
  IF_Non-Scheduling_Turn_v19.json    -> if_non_scheduling_turn_v19
  IF_Single_Agent_Result_Phase.json  -> if_single_agent_result_phase
  IF_Handoff_Required.json           -> if_handoff_required
  IF_Handoff_Active.json             -> if_handoff_active
  IF_Normalize_Error.json            -> if_normalize_error

  (Node names starting with "IF" drop that prefix in the function suffix; the
  mapping table above is authoritative.)

n8n v2 operator semantics (n8n/packages/workflow/src/node-parameters/filter-parameter.ts):
  - boolean 'true'   -> the parsed leftValue itself: condition is true iff
    leftValue === true. rightValue is ignored (singleValue operator), even when
    the extracted JSON carries rightValue: true.
  - string 'equals'  -> left === right (caseSensitive: true in every node here).
  - typeValidation 'strict': a leftValue of the wrong type fails validation and
    the condition is false (undefined/null also compare false). Ports never raise.
  - typeValidation 'loose' (IF_K2_Burst_Allowed only): the leftValue is coerced
    to the operator type — but its expression `$json.allowed === true` already
    evaluates to a real boolean, so loose vs strict is not observable there.

Every predicate reads only the fields the n8n condition actually compares; the
leftValue expressions are reproduced verbatim in each docstring.
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
    """n8n v2 boolean operator 'true' under strict validation: left === True."""
    return value is True


def if_check_duplicate_message(item: Dict[str, Any]) -> bool:
    """Source node: Check Duplicate Message (extracted/nodes/Check_Duplicate_Message.json).

    combinator 'and', typeValidation 'strict':
      leftValue `={{ $json.duplicate }}` with boolean operator 'true'
      -> condition holds iff item.duplicate is exactly True.
    """
    return _bool_true(_prop(item, "duplicate"))


def if_check_patient_ownership(item: Dict[str, Any]) -> bool:
    """Source node: Check Patient Ownership (extracted/nodes/Check_Patient_Ownership.json).

    combinator 'and', typeValidation 'strict':
      leftValue `={{ $json.ownership_valid }}` with boolean operator 'true'
      -> condition holds iff item.ownership_valid is exactly True.
    """
    return _bool_true(_prop(item, "ownership_valid"))


def if_early_security_reject(item: Dict[str, Any]) -> bool:
    """Source node: IF Early Security Reject (extracted/nodes/IF_Early_Security_Reject.json).

    combinator 'and', typeValidation 'strict':
      leftValue `={{ $json.security_reject }}` with boolean operator 'true'
      -> condition holds iff item.security_reject is exactly True.
    """
    return _bool_true(_prop(item, "security_reject"))


def if_k2_signature_accepted(item: Dict[str, Any]) -> bool:
    """Source node: IF K2 Signature Accepted (extracted/nodes/IF_K2_Signature_Accepted.json).

    combinator 'and', typeValidation 'strict':
      leftValue `={{ $json.accepted }}` with boolean operator 'true'
      (rightValue: true present in the JSON but ignored by the singleValue
      operator) -> condition holds iff item.accepted is exactly True.
    """
    return _bool_true(_prop(item, "accepted"))


def if_k2_burst_allowed(item: Dict[str, Any]) -> bool:
    """Source node: IF K2 Burst Allowed (extracted/nodes/IF_K2_Burst_Allowed.json).

    combinator 'and', typeValidation 'loose':
      leftValue `={{ $json.allowed === true }}` with boolean operator 'true'.
      The JS strict-equality expression already yields a boolean, so the loose
      boolean coercion is a no-op -> condition holds iff item.allowed is
      exactly True (JS `1 === true` is false; Python `1 is True` is False).
    """
    return _prop(item, "allowed") is True


def if_completed_create_replay(item: Dict[str, Any]) -> bool:
    """Source node: Completed Create Replay? (extracted/nodes/Completed_Create_Replay.json).

    combinator 'and', typeValidation 'strict':
      leftValue `={{ $json.replay_gate.matched }}` with boolean operator 'true'
      (rightValue: true ignored) -> n8n's expression proxy resolves the missing
      path to undefined, so the condition holds iff
      item.replay_gate.matched is exactly True.
    """
    replay_gate = _prop(item, "replay_gate")
    matched = _prop(replay_gate, "matched") if isinstance(replay_gate, dict) else _UNDEFINED
    return _bool_true(matched)


def if_non_scheduling_turn_v19(item: Dict[str, Any]) -> bool:
    """Source node: IF Non-Scheduling Turn (v19) (extracted/nodes/IF_Non-Scheduling_Turn_v19.json).

    combinator 'and', typeValidation 'strict':
      leftValue `={{ $json.system_decision && $json.system_decision.non_scheduling_turn === true }}`
      with boolean operator 'true' (rightValue: true ignored). The && yields
      undefined (non-boolean -> strict validation fails -> false) when
      system_decision is falsy, so the condition holds iff system_decision is a
      truthy object carrying non_scheduling_turn === true.
    """
    system_decision = _prop(item, "system_decision")
    if not isinstance(system_decision, dict):
        return False
    return bool(system_decision) and _bool_true(_prop(system_decision, "non_scheduling_turn"))


def if_handoff_required(item: Dict[str, Any]) -> bool:
    """Source node: IF Handoff Required (extracted/nodes/IF_Handoff_Required.json).

    combinator 'and', typeValidation 'strict':
      leftValue `={{ $json.system_decision && $json.system_decision.response_code || '' }}`
      (JS precedence: (a && b) || '') with string operator 'equals' and
      rightValue "HANDOFF_REQUIRED" (caseSensitive: true). The || '' guarantees a
      string leftValue, so the condition holds iff the decision response_code
      equals 'HANDOFF_REQUIRED'.
    """
    system_decision = _prop(item, "system_decision")
    response_code = (
        _prop(system_decision, "response_code")
        if isinstance(system_decision, dict)
        else _UNDEFINED
    )
    left = response_code if (isinstance(response_code, str) and response_code) else ""
    return _string_equals(left, "HANDOFF_REQUIRED")


def if_handoff_active(item: Dict[str, Any]) -> bool:
    """Source node: IF Handoff Active (extracted/nodes/IF_Handoff_Active.json).

    combinator 'and', typeValidation 'strict':
      leftValue `={{ !!$json.handoff_request_id }}` with boolean operator 'true'
      (rightValue: true ignored) -> !!x is a real boolean that is true iff
      handoff_request_id is JS-truthy (non-empty string, non-zero number, any
      object/array; '' / 0 / null / undefined are falsy).
    """
    value = _prop(item, "handoff_request_id")
    if value is None or value is _UNDEFINED or value is False:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return not (value == 0 or (isinstance(value, float) and value != value))
    if isinstance(value, str):
        return value != ""
    return True  # dict/list are always truthy in JS


def if_normalize_error(item: Dict[str, Any]) -> bool:
    """Source node: IF Normalize Error (extracted/nodes/IF_Normalize_Error.json).

    combinator 'and', typeValidation 'strict':
      leftValue `={{ $json.normalization_error === true }}` with boolean operator
      'true' (rightValue: true ignored) — the double-true collapses to
      item.normalization_error is True.
    """
    return _prop(item, "normalization_error") is True
