"""Verbatim SQL constants for the k2 agent — one per n8n Postgres node.

Source of truth: n8n_reference/sql_queries_reference.json (production copy of the
real queries), cross-checked byte-for-byte against
n8n_reference/extracted/sql/*.json (only trailing whitespace differs).

DO NOT reformat: the SQL text is the exact string the n8n Postgres node executed,
including comments, CTEs, Arabic literals and mojibake byte sequences. Parameter
placeholders ($1..$N) map 1:1 to each node's queryReplacement expression; the
binding order per node lives in app/db/repository.py.

QUERY_SAVE_CONVERSATION_STATE_RPC is the only port-added statement: it is the
direct Postgres equivalent of the n8n HTTP nodes "Save Conversation State" and
"Save Conversation State (retry) (v18)" (Supabase REST POST
/rest/v1/rpc/k2_save_conversation_state with body
{p_conversation_id, p_state_data, p_previous_state_version}).
"""

# Log Incoming Message (extracted/sql/Log_Incoming_Message.json)
QUERY_LOG_INCOMING_MESSAGE = r'''WITH eligible AS (
  SELECT
    $1::uuid AS conversation_id,
    $2::uuid AS clinic_id,
    $3::uuid AS patient_id,
    $4::text AS content,
    $5::timestamptz AS received_at,
    COALESCE($6::jsonb, '{}'::jsonb) AS metadata,
    md5($7::text)::uuid AS message_id,
    NULLIF($10::text, '') AS chat_id,
    EXISTS (SELECT 1 FROM conversations cb WHERE cb.id = $1::uuid AND cb.clinic_id = $2::uuid AND cb.channel_id = $8::uuid AND cb.deleted_at IS NULL AND ($10::text IS NULL OR $10::text = '' OR cb.channel_conversation_id = $10::text)) AS binding_matches,
    EXISTS (SELECT 1 FROM patients p WHERE p.id = $3::uuid AND p.clinic_id = $2::uuid) AS patient_exists,
    EXISTS (SELECT 1 FROM conversations c WHERE c.id = $1::uuid AND c.clinic_id = $2::uuid AND c.patient_id = $3::uuid) AS conversation_matches,
    EXISTS (SELECT 1 FROM channels ch WHERE ch.id = $8::uuid AND ch.clinic_id = $2::uuid AND ch.is_enabled = true AND ch.deleted_at IS NULL AND lower(ch.type) = lower($9::text)) AS channel_matches
), inserted AS (
  INSERT INTO messages (id, conversation_id, clinic_id, patient_id, sender_type, content, received_at, metadata, direction, message_status)
  SELECT message_id, conversation_id, clinic_id, patient_id, 'patient', content, received_at, metadata, 'incoming', 'delivered'
  FROM eligible
  WHERE patient_exists AND conversation_matches AND channel_matches AND binding_matches
  ON CONFLICT (id) DO NOTHING
  RETURNING id
)
SELECT
  (SELECT message_id FROM eligible) AS id,
  EXISTS(SELECT 1 FROM inserted) AS inserted,
  -- duplicate only when all three tenant checks passed and the id already existed;
  -- any failed check (patient/conversation/channel) is a security reject, never a duplicate.
  (SELECT patient_exists AND conversation_matches AND channel_matches FROM eligible) AND NOT EXISTS(SELECT 1 FROM inserted) AS duplicate,
  (NOT (SELECT patient_exists AND conversation_matches FROM eligible) OR NOT (SELECT channel_matches FROM eligible) OR NOT (SELECT binding_matches FROM eligible)) AS security_reject;'''

# Get Clinic Context (extracted/sql/Get_Clinic_Context.json)
QUERY_GET_CLINIC_CONTEXT = r'''WITH context AS (
  SELECT
    c.id AS clinic_id,
    NULLIF(c.timezone, '') AS clinic_timezone,
    c.name AS clinic_name,
    COALESCE(c.location_config, '{}'::jsonb) AS clinic_location_config,
    c.dialect_code AS clinic_dialect_code,
    COALESCE(NULLIF(cs.general_settings->>'country_code', ''), NULLIF(cs.ai_settings->>'country_code', ''), CASE WHEN c.dialect_code = 'saudi' THEN 'SA' ELSE NULL END) AS clinic_country_code,
    (SELECT b.phone FROM branches b WHERE b.clinic_id = c.id AND b.is_active = true ORDER BY b.created_at LIMIT 1) AS clinic_phone,
    COALESCE((
      SELECT JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'branch_id', b.id,
          'branch_name', b.name,
          'address', b.address,
          'phone', b.phone,
          'location_config', COALESCE(b.location_config, '{}'::jsonb)
        ) ORDER BY b.created_at
      )
      FROM branches b
      WHERE b.clinic_id = c.id
        AND b.is_active = true
    ), '[]'::jsonb) AS branch_directory,
    CASE WHEN p.id IS NOT NULL THEN p.name ELSE NULL END AS patient_name,
    CASE WHEN p.id IS NOT NULL THEN p.phone ELSE NULL END AS patient_phone,
    CASE WHEN p.id IS NOT NULL THEN p.age ELSE NULL END AS patient_age,
    CASE WHEN p.id IS NOT NULL THEN p.address ELSE NULL END AS patient_address,
    CASE WHEN p.id IS NOT NULL THEN conv.patient_id ELSE NULL END AS conversation_patient_id,
    (p.id IS NOT NULL) AS ownership_valid,
    COALESCE(cs.general_settings, '{}'::jsonb) AS settings,
    COALESCE(cs.ai_settings->'persona', '{"name":"نور","role":"مساعد حجوزات","tone":"warm","dialect":"saudi"}'::jsonb) AS persona,
    COALESCE(cs.ai_settings->>'prompt_version', 'v1') AS prompt_version,
    COALESCE(cs.ai_settings->>'system_prompt', '') AS clinic_system_prompt,
    COALESCE(NULLIF(cs.ai_settings->>'confidence_threshold', '')::numeric, 0.75) AS confidence_threshold,
    CASE WHEN COALESCE(cs.general_settings->>'confirmation_ttl_seconds', '') ~ '^[0-9]+$' THEN GREATEST(60, LEAST(3600, (cs.general_settings->>'confirmation_ttl_seconds')::integer)) ELSE 600 END AS confirmation_ttl_seconds,
    CASE WHEN COALESCE(cs.general_settings->>'draft_ttl_seconds', '') ~ '^[0-9]+$' THEN GREATEST(60, LEAST(86400, (cs.general_settings->>'draft_ttl_seconds')::integer)) ELSE 1800 END AS draft_ttl_seconds,
    COALESCE(doctor_scope.doctor_count, 0) AS doctor_count,
    CASE WHEN COALESCE(doctor_scope.doctor_count, 0) = 1 THEN doctor_scope.single_doctor_id ELSE NULL END AS single_doctor_id,
    CASE WHEN COALESCE(doctor_scope.doctor_count, 0) = 1 THEN doctor_scope.single_doctor_name ELSE NULL END AS single_doctor_name,
    COALESCE(doctor_scope.doctor_directory, '[]'::jsonb) AS doctor_directory,
    true AS clinic_found
  FROM clinics c
  LEFT JOIN conversations conv ON conv.id = $1::uuid AND conv.clinic_id = $2::uuid
  LEFT JOIN patients p ON p.id = conv.patient_id AND p.id = $3::uuid AND p.clinic_id = $2::uuid
  LEFT JOIN clinic_settings cs ON cs.clinic_id = c.id
  LEFT JOIN LATERAL (
    SELECT
      COUNT(*)::int AS doctor_count,
      (ARRAY_AGG(d.id ORDER BY d.name))[1] AS single_doctor_id,
      (ARRAY_AGG(d.name ORDER BY d.name))[1] AS single_doctor_name,
      COALESCE(
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'doctor_id', d.id,
            'doctor_name', d.name,
            'specialization', d.specialization
          ) ORDER BY d.name
        ),
        '[]'::jsonb
      ) AS doctor_directory
    FROM doctors d
    WHERE d.clinic_id = c.id
      AND d.is_active = true
      AND d.deleted_at IS NULL
  ) doctor_scope ON true
  WHERE c.id = $2::uuid
), fallback AS (
  SELECT
    $2::uuid AS clinic_id,
    NULL::text AS clinic_timezone,
    NULL::text AS clinic_name,
    '{}'::jsonb AS clinic_location_config,
    NULL::text AS clinic_dialect_code,
    NULL::text AS clinic_country_code,
    NULL::text AS clinic_phone,
    '[]'::jsonb AS branch_directory,
    NULL::text AS patient_name,
    NULL::text AS patient_phone,
    NULL::integer AS patient_age,
    NULL::text AS patient_address,
    NULL::uuid AS conversation_patient_id,
    false AS ownership_valid,
    '{}'::jsonb AS settings,
    '{"name":"نور","role":"مساعد حجوزات","tone":"warm","dialect":"saudi"}'::jsonb AS persona,
    'v1'::text AS prompt_version,
    ''::text AS clinic_system_prompt,
    0.75::numeric AS confidence_threshold,
    600::integer AS confirmation_ttl_seconds,
    1800::integer AS draft_ttl_seconds,
    0::integer AS doctor_count,
    NULL::uuid AS single_doctor_id,
    NULL::text AS single_doctor_name,
    '[]'::jsonb AS doctor_directory,
    false AS clinic_found
)
SELECT * FROM context
UNION ALL
SELECT * FROM fallback WHERE NOT EXISTS (SELECT 1 FROM context)
LIMIT 1;'''

# Get Conversation State (extracted/sql/Get_Conversation_State.json)
QUERY_GET_CONVERSATION_STATE = r'''WITH eligible_states AS (
  SELECT cs.conversation_id, cs.state_data, (cs.conversation_id = $1::uuid) AS is_current
  FROM conversation_state cs
  JOIN conversations c ON c.id = cs.conversation_id
  WHERE c.clinic_id = $2::uuid AND c.patient_id = $3::uuid AND c.deleted_at IS NULL
), current_state AS (
  SELECT state_data FROM eligible_states WHERE is_current LIMIT 1
), merged_facts AS (
  SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb) AS facts
  FROM (
    SELECT key, value, ROW_NUMBER() OVER (PARTITION BY key ORDER BY is_current DESC) AS rn
    FROM eligible_states, LATERAL jsonb_each(COALESCE(state_data->'facts', '{}'::jsonb))
    WHERE key IN ('clinic', 'patient')
  ) fact_rows
  WHERE rn = 1
), raw_state AS (
  SELECT COALESCE((SELECT state_data FROM current_state), '{}'::jsonb) || jsonb_build_object(
    'facts', (SELECT facts FROM merged_facts)
  ) AS sd
), flags AS (
  SELECT sd,
    (
      (COALESCE(sd->>'draft_expires_at', '') ~ '^\d{4}-\d{2}-\d{2}T'
        AND (sd->>'draft_expires_at')::timestamptz <= now())
      OR (
        COALESCE(sd->>'operation_state', '') IN ('DRAFT','COLLECTING_DETAILS','COLLECTING_APPOINTMENT_DETAILS')
        AND COALESCE(sd->>'draft_expires_at', '') = ''
        AND COALESCE(sd->>'last_updated', '') ~ '^\d{4}-\d{2}-\d{2}T'
        AND (sd->>'last_updated')::timestamptz <= now() - interval '30 minutes'
      )
    ) AS draft_expired,
    (
      COALESCE(sd#>>'{booking_context,patient_address}', '') ~ '^\{'
      OR COALESCE(sd#>>'{booking_context,patient_address}', '') ~ '^\['
      OR COALESCE(sd#>>'{facts,patient,address}', '') ~ '^\{'
      OR COALESCE(sd#>>'{facts,patient,address}', '') ~ '^\['
      OR length(COALESCE(sd#>>'{booking_context,patient_address}', '')) > 600
    ) AS address_polluted
  FROM raw_state
), scrubbed AS (
  SELECT
    CASE
      WHEN f.draft_expired THEN jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(
        f.sd,
        '{booking_context,date}', 'null'::jsonb, true),
        '{booking_context,time}', 'null'::jsonb, true),
        '{booking_context,slot_id}', 'null'::jsonb, true),
        '{slot_state,date}', 'null'::jsonb, true),
        '{slot_state,time}', 'null'::jsonb, true),
        '{slot_state,slot_id}', 'null'::jsonb, true),
        '{facts,booking,date}', 'null'::jsonb, true),
        '{facts,booking,time}', 'null'::jsonb, true)
      ELSE f.sd
    END AS sd,
    f.draft_expired,
    f.address_polluted
  FROM flags f
), cleaned AS (
  SELECT
    CASE
      WHEN s.draft_expired AND COALESCE(s.sd->>'operation_state', '') IN ('DRAFT','COLLECTING_DETAILS','COLLECTING_APPOINTMENT_DETAILS','COLLECTING_PATIENT_DATA') THEN jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(
        s.sd,
        '{operation_state}', '"IDLE"'::jsonb, true),
        '{operation_status}', '"idle"'::jsonb, true),
        '{active_operation}', 'null'::jsonb, true),
        '{operation_id}', 'null'::jsonb, true),
        '{routing_action}', 'null'::jsonb, true),
        '{pending_action}', 'null'::jsonb, true),
        '{confirmation_state}', 'null'::jsonb, true),
        '{draft_started_at}', 'null'::jsonb, true),
        '{draft_expires_at}', 'null'::jsonb, true)
      ELSE s.sd
    END AS sd,
    s.address_polluted
  FROM scrubbed s
)
SELECT $1::uuid AS conversation_id,
  CASE
    WHEN c.address_polluted THEN jsonb_set(jsonb_set(jsonb_set(
      c.sd,
      '{booking_context,patient_address}', 'null'::jsonb, true),
      '{facts,patient,address}', 'null'::jsonb, true),
      '{patient_data_review,fields,address}', 'null'::jsonb, true)
    ELSE c.sd
  END AS state_data
FROM cleaned c;'''

# Log Outgoing Message (extracted/sql/Log_Outgoing_Message.json)
QUERY_LOG_OUTGOING_MESSAGE = r'''WITH candidate AS (
  SELECT md5(($1 || ':outgoing')::text)::uuid AS message_id, $2::uuid AS conversation_id, $3::uuid AS clinic_id, $4::uuid AS patient_id, $5::text AS content, $6::timestamptz AS sent_at, COALESCE($7::jsonb, '{}'::jsonb) AS metadata, $8::text AS user_message_id
),
inserted AS (
  INSERT INTO messages (id, conversation_id, clinic_id, patient_id, sender_type, content, received_at, metadata, direction, message_status, llm_model, ai_tokens)
  SELECT message_id, conversation_id, clinic_id, patient_id, 'ai', content, sent_at, metadata, 'outgoing', 'sent', $9::text, NULLIF($10::text,'null')::integer FROM candidate
  ON CONFLICT (id) DO NOTHING
  RETURNING id
),
outgoing_row AS (
  SELECT id AS message_id FROM inserted
  UNION
  SELECT c.message_id FROM candidate c JOIN messages m ON m.id = c.message_id
),
state_updated AS (
  UPDATE conversation_state cs
  SET state_data = jsonb_set(
    jsonb_set(
      jsonb_set(cs.state_data, '{confirmation_target,confirmation_prompt_message_id}', to_jsonb(o.message_id::text), true),
      '{confirmation_target,confirmation_delivery_status}', to_jsonb('sent'::text), true
    ),
    '{confirmation_target,confirmation_delivery_recorded_at}', to_jsonb(c.sent_at), true
  )
  FROM outgoing_row o
  JOIN candidate c ON c.message_id = o.message_id
  WHERE cs.conversation_id = c.conversation_id
    AND jsonb_typeof(cs.state_data -> 'confirmation_target') = 'object'
    AND cs.state_data -> 'confirmation_target' ->> 'last_user_message_id_at_request' = c.user_message_id

  RETURNING cs.conversation_id
)
SELECT o.message_id AS id, 'sent' AS delivery_status FROM outgoing_row o;'''

# Log Agent Audit Entry (extracted/sql/Log_Agent_Audit_Entry.json)
QUERY_LOG_AGENT_AUDIT_ENTRY = r'''WITH audit AS (
  INSERT INTO agent_audit_log (
    conversation_id, clinic_id, patient_id, message_text, intent, operation_status,
    escalate, appointment_id, reply_text, model, tool_calls, tool_call_count, total_time_ms, received_at
  ) VALUES (
    NULLIF($1::text,'null')::uuid,
    NULLIF($2::text,'null')::uuid,
    NULLIF($3::text,'null')::uuid,
    $4,
    NULLIF($5::text,'null'),
    NULLIF($6::text,'null'),
    COALESCE(NULLIF($7::text,'null')::boolean, false),
    NULLIF($8::text,'null')::uuid,
    $9,
    NULLIF($10::text,'null'),
    COALESCE(NULLIF($11::text,'null'),'[]')::jsonb,
    COALESCE(NULLIF($12::text,'null')::integer, 0),
    NULLIF($13::text,'null')::integer,
    NULLIF($14::text,'null')::timestamptz
  )
  RETURNING id
), failure AS (
  INSERT INTO public.understanding_failures (
    clinic_id, conversation_id, patient_id, channel_type, message_text,
    model_intent, model_confidence, failure_types, draft_live, response_code
  )
  SELECT
    NULLIF($2::text,'null')::uuid,
    NULLIF($1::text,'null')::uuid,
    NULLIF($3::text,'null')::uuid,
    NULLIF($15::text,'null'),
    $4,
    NULLIF($16::text,'null'),
    NULLIF($17::text,'null')::numeric,
    COALESCE(NULLIF($18::text,'null')::jsonb, '[]'::jsonb),
    COALESCE(NULLIF($19::text,'null')::boolean, false),
    NULLIF($20::text,'null')
  WHERE jsonb_array_length(COALESCE(NULLIF($18::text,'null')::jsonb, '[]'::jsonb)) > 0
  RETURNING id
)
SELECT (SELECT count(*) FROM audit) AS audit_rows, (SELECT count(*) FROM failure) AS failure_rows;'''

# Execute Approved Create Appointment (extracted/sql/Execute_Approved_Create_Appointment.json)
QUERY_EXECUTE_APPROVED_CREATE_APPOINTMENT = r'''SELECT
  r.appointment_id AS id,
  r.booking_id,
  r.branch_id,
  r.patient_profile_update,
  r.booking_number,
  r.queue_number,
  r.queue_path,
  r.queue_expires_at,
  json_build_object(
    'schema_version', 1,
    'operation', 'create_appointment',
    'correlation_id', COALESCE(NULLIF($7::text, ''), NULLIF($8::text, '')),
    'operation_id', NULLIF($8::text, ''),
    'response_code', 'CREATE_COMPLETED',
    'success', true,
    'retryable', false,
    'appointment_id', r.appointment_id,
    'booking_id', r.booking_id,
    'booking_number', r.booking_number,
    'branch_id', r.branch_id,
    'queue_number', r.queue_number,
    'queue_path', r.queue_path,
    'queue_expires_at', r.queue_expires_at,
    'appointment_type', COALESCE(NULLIF($6::text, ''), 'NEW_VISIT'),
    'patient_profile_update', r.patient_profile_update
  ) AS child_response
FROM public.create_appointment_with_queue_link(
  $1::uuid,
  $2::uuid,
  NULLIF($3::text, '')::uuid,
  $4::text,
  NULLIF($5::text, ''),
  COALESCE(NULLIF($6::text, ''), 'NEW_VISIT'),
  NULLIF($8::text, ''),
  NULL,
  $9::uuid,
  NULLIF($10::text, ''),
  NULLIF($11::text, ''),
  NULLIF($12::text, '')::integer,
  NULLIF($13::text, '')
) AS r;'''

# Execute Approved Cancel Appointment (extracted/sql/Execute_Approved_Cancel_Appointment.json)
QUERY_EXECUTE_APPROVED_CANCEL_APPOINTMENT = r'''WITH params AS (
  SELECT
    $1::uuid AS clinic_id,
    $2::uuid AS patient_id,
    NULLIF($3::text, '')::uuid AS appointment_id,
    NULLIF($4::text, '') AS cancellation_reason,
    NULLIF($5::text, '') AS operation_id,
    $6 AS correlation_id
), target AS (
  SELECT a.id, a.appointment_status
  FROM public.appointments a, params p
  WHERE a.id = p.appointment_id
    AND a.clinic_id = p.clinic_id
    AND a.patient_id = p.patient_id
    AND a.deleted_at IS NULL
  LIMIT 1
), decision AS (
  SELECT p.*, t.id AS target_id, t.appointment_status,
    CASE
      WHEN t.id IS NULL THEN 'APPOINTMENT_NOT_FOUND_OR_NOT_OWNED'
      WHEN t.appointment_status IN ('completed','no_show','cancelled') THEN 'CANCELLATION_NOT_ALLOWED'
      ELSE NULL
    END AS precondition_code
  FROM params p LEFT JOIN target t ON true
), mutated AS (
  SELECT d.*, CASE WHEN d.precondition_code IS NULL THEN public.rpc_cancel_appointment_v2(d.target_id, d.clinic_id, d.patient_id, d.cancellation_reason, NULL) ELSE NULL END AS rpc_ok
  FROM decision d
)
SELECT
  1 AS schema_version,
  'cancel_appointment' AS operation,
  correlation_id,
  operation_id,
  CASE WHEN precondition_code IS NOT NULL THEN precondition_code WHEN rpc_ok IS TRUE THEN 'CANCEL_COMPLETED' ELSE 'RPC_ERROR' END AS response_code,
  (precondition_code IS NULL AND rpc_ok IS TRUE) AS success,
  false AS retryable,
  CASE WHEN precondition_code IS NULL AND rpc_ok IS TRUE THEN target_id::text ELSE NULL END AS appointment_id,
  CASE WHEN precondition_code IS NOT NULL THEN precondition_code WHEN rpc_ok IS TRUE THEN NULL ELSE 'RPC_CANCEL_FALSE' END AS error_code,
  CASE WHEN precondition_code IS NULL AND rpc_ok IS TRUE THEN 'EXECUTED' ELSE 'NOT_EXECUTED' END AS mutation_status
FROM mutated;'''

# Execute Approved Reschedule Appointment (extracted/sql/Execute_Approved_Reschedule_Appointment.json)
QUERY_EXECUTE_APPROVED_RESCHEDULE_APPOINTMENT = r'''WITH params AS (
  SELECT
    $1::uuid AS clinic_id,
    $2::uuid AS patient_id,
    $3::uuid AS conversation_id,
    NULLIF($4::text, '')::uuid AS appointment_id,
    NULLIF($5::text, '')::uuid AS expected_old_slot_id,
    NULLIF($6::text, '')::uuid AS new_slot_id,
    NULLIF($7::text, '') AS operation_id,
    $8 AS correlation_id
), rpc_result AS (
  SELECT public.rpc_reschedule_appointment(
    p.appointment_id,
    p.clinic_id,
    p.patient_id,
    p.conversation_id,
    p.expected_old_slot_id,
    p.new_slot_id,
    p.operation_id
  ) AS body, p.*
  FROM params p
  WHERE p.appointment_id IS NOT NULL
    AND p.expected_old_slot_id IS NOT NULL
    AND p.new_slot_id IS NOT NULL
    AND p.operation_id IS NOT NULL
), normalized AS (
  SELECT
    COALESCE((body->>'response_code'), 'RPC_ERROR') AS response_code,
    COALESCE((body->>'success')::boolean, false) AS success,
    COALESCE((body->>'retryable')::boolean, false) AS retryable,
    NULLIF(body->>'appointment_id','') AS appointment_id,
    COALESCE(body->>'error_code', CASE WHEN body IS NULL THEN 'INVALID_INPUT' ELSE NULL END) AS error_code,
    operation_id, correlation_id
  FROM rpc_result
), fallback AS (
  SELECT
    'RESCHEDULE_NOT_ALLOWED' AS response_code,
    false AS success,
    false AS retryable,
    NULL::text AS appointment_id,
    'INVALID_INPUT' AS error_code,
    p.operation_id, p.correlation_id
  FROM params p
  WHERE p.appointment_id IS NULL OR p.expected_old_slot_id IS NULL OR p.new_slot_id IS NULL OR p.operation_id IS NULL
)
SELECT
  1 AS schema_version,
  'reschedule_appointment' AS operation,
  correlation_id,
  operation_id,
  response_code,
  success,
  retryable,
  appointment_id,
  error_code,
  CASE WHEN success THEN 'EXECUTED' WHEN retryable THEN 'NOT_EXECUTED' ELSE 'NOT_EXECUTED' END AS mutation_status
FROM normalized
UNION ALL
SELECT
  1, 'reschedule_appointment', correlation_id, operation_id, response_code, success, retryable, appointment_id, error_code, 'NOT_EXECUTED'
FROM fallback;'''

# Resolve Booking IDs (Deterministic) (extracted/sql/Resolve_Booking_IDs_Deterministic.json)
QUERY_RESOLVE_BOOKING_IDS_DETERMINISTIC = r'''WITH input AS (
  SELECT
    $1::uuid AS clinic_id,
    $2::uuid AS patient_id,
    $3::uuid AS conversation_id,
    NULLIF($4::text,'') AS requested_appointment_ref,
    CASE WHEN NULLIF($4::text,'') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      THEN NULLIF($4::text,'')::uuid ELSE NULL END AS requested_appointment_id,
    CASE WHEN NULLIF($5::text,'') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      THEN NULLIF($5::text,'')::uuid ELSE NULL END AS requested_old_slot_id,
    CASE WHEN NULLIF($6::text,'') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      THEN NULLIF($6::text,'')::uuid ELSE NULL END AS requested_new_slot_id,
    CASE WHEN NULLIF($7::text,'') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      THEN NULLIF($7::text,'')::uuid ELSE NULL END AS requested_doctor_id,
    CASE WHEN NULLIF($8::text,'') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      THEN NULLIF($8::text,'')::uuid ELSE NULL END AS requested_service_id,
    NULLIF($9::text,'') AS requested_date_text,
    CASE WHEN NULLIF($9::text,'') ~ '^\d{4}-\d{2}-\d{2}$' THEN NULLIF($9::text,'')::date ELSE NULL END AS requested_date,
    NULLIF($10::text,'') AS requested_time_text,
    CASE WHEN NULLIF($10::text,'') ~ '^\d{2}:\d{2}(:\d{2})?$' THEN NULLIF($10::text,'')::time ELSE NULL END AS requested_time,
    (NULLIF($9::text,'') IS NOT NULL AND NULLIF($9::text,'') !~ '^\d{4}-\d{2}-\d{2}$') AS invalid_requested_date,
    (NULLIF($10::text,'') IS NOT NULL AND NULLIF($10::text,'') !~ '^\d{2}:\d{2}(:\d{2})?$') AS invalid_requested_time,
    NULLIF(trim(regexp_replace(
      regexp_replace(translate(lower(trim($11::text)), 'أإآ', 'ااا'), '(^|[[:space:]])(ال)?(دكتورة|دكتور|د\.?)([[:space:]]|$)', ' ', 'gi'),
      '\s+', ' ', 'g'
    )), '') AS requested_doctor_name,
    NULLIF(trim(regexp_replace(
      regexp_replace(lower(trim($12::text)), '(^|[[:space:]])(ال)?(اسنان|اسنان|كشف|فحص|تنظيف|حشو|جذور|تقويم|زراعة|زراعه)([[:space:]]|$)', ' ', 'gi'),
      '\s+', ' ', 'g'
    )), '') AS requested_service_name,
    NULLIF($13::text,'') AS operation_type,
    NULLIF(btrim($14::text), '') AS requested_booking_number
), doctor_pool AS (
  SELECT d.id::text AS id, d.name,
    trim(regexp_replace(
      regexp_replace(translate(lower(trim(d.name)), 'أإآ', 'ااا'), '(^|[[:space:]])(ال)?(دكتورة|دكتور|د\.?)([[:space:]]|$)', ' ', 'gi'),
      '\s+', ' ', 'g'
    )) AS normalized_name,
    i.requested_doctor_id, i.requested_doctor_name
  FROM public.doctors d CROSS JOIN input i
  WHERE d.clinic_id = i.clinic_id AND d.is_active = true AND d.deleted_at IS NULL
), doctor_matches AS (
  SELECT id, name
  FROM doctor_pool
  WHERE (requested_doctor_id IS NOT NULL AND id::uuid = requested_doctor_id)
     OR (requested_doctor_id IS NULL AND (requested_doctor_name IS NULL
       OR normalized_name = requested_doctor_name
       OR normalized_name ILIKE '%' || requested_doctor_name || '%'
       OR requested_doctor_name ILIKE '%' || normalized_name || '%'))
), service_pool AS (
  SELECT s.id::text AS id, s.name,
    trim(regexp_replace(
      regexp_replace(lower(trim(s.name)), '(^|[[:space:]])(ال)?(اسنان|اسنان|كشف|فحص|تنظيف|حشو|جذور|تقويم|زراعة|زراعه)([[:space:]]|$)', ' ', 'gi'),
      '\s+', ' ', 'g'
    )) AS normalized_name,
    i.requested_service_id, i.requested_service_name
  FROM public.services s CROSS JOIN input i
  WHERE s.clinic_id = i.clinic_id AND s.is_active = true AND s.deleted_at IS NULL AND s.online_booking = true
), service_matches AS (
  SELECT id, name
  FROM service_pool
  WHERE (requested_service_id IS NOT NULL AND id::uuid = requested_service_id)
     OR (requested_service_id IS NULL AND (
       requested_service_name IS NULL
       OR normalized_name = requested_service_name
       OR (
         NOT EXISTS (
           SELECT 1
           FROM service_pool exact_match
           WHERE exact_match.normalized_name = service_pool.requested_service_name
         )
         AND (
           normalized_name ILIKE '%' || requested_service_name || '%'
           OR requested_service_name ILIKE '%' || normalized_name || '%'
         )
       )
     ))
), owned_candidates AS (
  SELECT a.id, a.public_id, a.booking_number, a.patient_id, a.clinic_id, a.doctor_id, a.service_id, a.branch_id, a.slot_id, a.scheduled_at, a.appointment_status,
    count(*) OVER () AS match_count
  FROM public.appointments a CROSS JOIN input i
  WHERE a.clinic_id = i.clinic_id AND a.patient_id = i.patient_id AND a.deleted_at IS NULL
    AND a.appointment_status IN ('scheduled','confirmed')
    AND (
      (i.requested_booking_number IS NOT NULL AND a.booking_number = i.requested_booking_number)
      OR (i.requested_booking_number IS NULL AND (
        i.requested_appointment_ref IS NULL
        OR (i.requested_appointment_id IS NOT NULL AND a.id = i.requested_appointment_id)
        OR (i.requested_appointment_id IS NULL AND (a.public_id = i.requested_appointment_ref OR a.booking_number = i.requested_appointment_ref))
      ))
    )
  ORDER BY a.scheduled_at ASC
  LIMIT 5
), selected_appointment AS (
  SELECT * FROM owned_candidates
  WHERE match_count = 1 OR (SELECT requested_appointment_ref FROM input) IS NOT NULL OR (SELECT requested_booking_number FROM input) IS NOT NULL
  LIMIT 1
), slot_candidates AS (
  SELECT s.id::text AS slot_id, s.branch_id, s.start_time, s.end_time, s.slot_status,
    count(*) OVER () AS match_count
  FROM public.appointment_slots s CROSS JOIN input i
  LEFT JOIN selected_appointment a ON true
  WHERE s.clinic_id = i.clinic_id AND s.deleted_at IS NULL AND s.slot_status = 'available' AND s.start_time >= now()
    AND NOT i.invalid_requested_date AND NOT i.invalid_requested_time
    AND (i.requested_new_slot_id IS NULL OR s.id = i.requested_new_slot_id)
    AND (a.id IS NULL OR (s.doctor_id = a.doctor_id AND s.service_id = a.service_id AND (a.branch_id IS NULL OR s.branch_id = a.branch_id)))
    AND (i.requested_doctor_id IS NULL OR s.doctor_id = i.requested_doctor_id)
    AND (i.requested_service_id IS NULL OR s.service_id = i.requested_service_id)
    AND (i.requested_date IS NULL OR s.start_time::date = i.requested_date)
  ORDER BY CASE WHEN i.requested_time IS NULL THEN 0 ELSE ABS(EXTRACT(EPOCH FROM s.start_time - (i.requested_date::timestamp + i.requested_time))) END, s.start_time
  LIMIT 5
), selected_slot AS (
  SELECT * FROM slot_candidates WHERE (SELECT requested_new_slot_id FROM input) IS NOT NULL OR (SELECT requested_date FROM input) IS NOT NULL OR (SELECT requested_time FROM input) IS NOT NULL LIMIT 1
)
SELECT
  (SELECT count(*) FROM doctor_matches)::int AS doctor_match_count,
  (SELECT count(*) FROM service_matches)::int AS service_match_count,
  ((SELECT count(*) FROM doctor_matches) > 1) AS doctor_ambiguous,
  ((SELECT count(*) FROM service_matches) > 1) AS service_ambiguous,
  CASE WHEN (SELECT count(*) FROM doctor_matches) = 1 THEN (SELECT id FROM doctor_matches LIMIT 1) ELSE NULL END AS doctor_id,
  CASE WHEN (SELECT count(*) FROM doctor_matches) = 1 THEN (SELECT name FROM doctor_matches LIMIT 1) ELSE NULL END AS doctor_name,
  CASE WHEN (SELECT count(*) FROM service_matches) = 1 THEN (SELECT id FROM service_matches LIMIT 1) ELSE NULL END AS service_id,
  CASE WHEN (SELECT count(*) FROM service_matches) = 1 THEN (SELECT name FROM service_matches LIMIT 1) ELSE NULL END AS service_name,
  COALESCE((SELECT jsonb_agg(jsonb_build_object('id', id, 'name', name) ORDER BY name) FROM doctor_matches), '[]'::jsonb) AS doctor_matches,
  COALESCE((SELECT jsonb_agg(jsonb_build_object('id', id, 'name', name) ORDER BY name) FROM service_matches), '[]'::jsonb) AS service_matches,
  (SELECT id::text FROM selected_appointment LIMIT 1) AS appointment_id,
  (SELECT booking_number FROM selected_appointment LIMIT 1) AS booking_number,
  ((SELECT match_count FROM owned_candidates LIMIT 1) > 1 AND (SELECT requested_appointment_ref FROM input) IS NULL) AS appointment_ambiguous,
  (SELECT slot_id FROM selected_appointment LIMIT 1) AS expected_old_slot_id,
  (SELECT slot_id FROM selected_slot LIMIT 1) AS new_slot_id,
  (SELECT slot_id FROM selected_slot LIMIT 1) AS slot_id,
  (SELECT branch_id FROM selected_slot LIMIT 1) AS branch_id,
  (SELECT start_time FROM selected_slot LIMIT 1) AS resolved_slot_start_time,
  (SELECT end_time FROM selected_slot LIMIT 1) AS resolved_slot_end_time,
  (SELECT operation_type FROM input) AS operation_type,
  CASE WHEN (SELECT invalid_requested_date FROM input) THEN 'INVALID_DATE'
       WHEN (SELECT invalid_requested_time FROM input) THEN 'INVALID_TIME'
       ELSE NULL END AS error_code;'''

# Resolve Doctor Inquiry (Deterministic) (extracted/sql/Resolve_Doctor_Inquiry_Deterministic.json)
QUERY_RESOLVE_DOCTOR_INQUIRY_DETERMINISTIC = r'''WITH extracted AS (
  SELECT
    $1::uuid AS clinic_id,
    $2::text AS message_text,
    CASE
      WHEN $2::text ~* '(?:اسماء|أسماء|قائمة|قائمه|من[[:space:]]+هم|مين|which|what|who|list|show|available).{0,48}(?:الدكاترة|الدكاتره|دكاترة|دكاتره|الأطباء|الاطباء|أطباء|اطباء|doctor|doctors|physician|specialist)'
        OR $2::text ~* '(?:الدكاترة|الدكاتره|دكاترة|دكاتره|الأطباء|الاطباء|أطباء|اطباء|doctor|doctors|physician|specialist).{0,48}(?:الموجودين|المتاحين|عندكم|في[[:space:]]+العيادة|في[[:space:]]+العياده|بالعيادة|بالعياده|available|there|at[[:space:]]+the[[:space:]]+clinic)'
      THEN true ELSE false
    END AS is_doctor_catalog_inquiry,
    CASE
      WHEN $2::text ~* '(?:^|[[:space:]])هل[[:space:]]*(?:الدكتور|دكتور|د\.|الطبيب|طبيب)[[:space:]]+'
        OR $2::text ~* '(?:^|[[:space:]])(?:الدكتور|دكتور|د\.|الطبيب|طبيب)[[:space:]]+[^؟?!،,.]+[[:space:]]+(?:موجود(?:ة)?|متاح(?:ة)?|متوفر(?:ة)?)'
        OR $2::text ~* '(?:^|[[:space:]])(?:is[[:space:]]+)?(?:dr\.?|doctor)[[:space:]]+[^?!,.]+[[:space:]]+(?:available|there|at[[:space:]]+the[[:space:]]+clinic)'
        OR $2::text ~* '(?:^|[[:space:]])(?:لا|مش|مو|ما)[[:space:]]*(?:اقصد|أقصد|قصدي|اعني|أعني)|(?:^|[[:space:]])(?:انا|أنا)[[:space:]]*(?:اقصد|أقصد|قصدي|اعني|أعني)|(?:^|[[:space:]])(?:i[[:space:]]+mean|no[[:space:]]+i[[:space:]]+mean|not[[:space:]]+(?:dr\.?|doctor)|rather|instead|actually)[[:space:]]+|(?:^|[[:space:]])(?:la2?[[:space:]]+(?:a2sed|ased|asdy|2asdy)|mesh[[:space:]]+(?:a2sed|ased|asdy|2asdy)|msh[[:space:]]+(?:a2sed|ased|asdy|2asdy)|2asdy|asdy)[[:space:]]+'
        OR $2::text ~* '(?:^|[[:space:]])(?:مع|بحجز|حجز[[:space:]]+مع|عايز|عاوز|عايزه|عاوزه|أبغى|ابغى|أريد|اريد|غيّر|غير|بدل|بدّل|بغيت|ودي)[[:space:]]*(?:الدكتورة|دكتوره|طبيبة|طبيبه|الدكتور|دكتور|د[.]|الطبيب|طبيب|dr[.]?|doctor)[[:space:]]+[^؟?!،,.]+'
        OR $2::text ~* '(?:^|[[:space:]])(?:الدكتورة|دكتوره|طبيبة|طبيبه|الدكتور|دكتور|د[.]|الطبيب|طبيب|dr[.]?|doctor)[[:space:]]+[^؟?!،,.]+[[:space:]]*(?:$|[[:space:]]+(?:يوم|بكرة|بكره|الاحد|الأحد|الاثنين|الثلاثاء|الاربع|الأربع|الاربعاء|الأربعاء|الخميس|الجمعة|الجمعه|السبت|الساعة|الساعه|عند|في))'
      THEN true ELSE false
    END AS is_doctor_inquiry,
    CASE
      WHEN $2::text ~* '[،,][[:space:]]*(?:اقصد|أقصد|اعني|أعني|i[[:space:]]+mean|rather|instead)[[:space:]]+'
        THEN NULLIF(TRIM(REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              $2::text,
              '^.*?[،,][[:space:]]*(?:اقصد|أقصد|اعني|أعني|i[[:space:]]+mean|rather|instead)[[:space:]]*', '', 'i'
            ),
            '^(?:الدكتورة|دكتوره|طبيبة|طبيبه|الدكتور|دكتور|د\.?|الطبيب|طبيب|dr\.?|doctor)[[:space:]]*', '', 'i'
          ),
          '[؟?!،,.].*$', '', 'i'
        )), '')
      WHEN $2::text ~* '(?:بل|قصدي|يعني|but|rather|instead)[[:space:]]*(?:الدكتورة|دكتوره|طبيبة|طبيبه|الدكتور|دكتور|د\.|الطبيب|طبيب|dr\.?|doctor)?[[:space:]]*[^؟?!،,.]+'
        THEN NULLIF(TRIM(REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE($2::text, '^.*?(?:بل|قصدي|يعني|but|rather|instead)[[:space:]]*', '', 'i'),
            '^(?:الدكتورة|دكتوره|طبيبة|طبيبه|الدكتور|دكتور|د\.|الطبيب|طبيب|dr\.?|doctor)[[:space:]]*', '', 'i'
          ),
          '[؟?!،,.].*$', '', 'i'
        )), '')
      WHEN $2::text ~* '(?:^|[[:space:]])(?:لا|مش|مو|ما)[[:space:]]*(?:اقصد|أقصد|قصدي|اعني|أعني)|(?:^|[[:space:]])(?:انا|أنا)[[:space:]]*(?:اقصد|أقصد|قصدي|اعني|أعني)|(?:^|[[:space:]])(?:i[[:space:]]+mean|no[[:space:]]+i[[:space:]]+mean|not[[:space:]]+(?:dr\.?|doctor)|rather|instead|actually)[[:space:]]+|(?:^|[[:space:]])(?:la2?[[:space:]]+(?:a2sed|ased|asdy|2asdy)|mesh[[:space:]]+(?:a2sed|ased|asdy|2asdy)|msh[[:space:]]+(?:a2sed|ased|asdy|2asdy)|2asdy|asdy)[[:space:]]+'
        THEN NULLIF(TRIM(REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              $2::text,
              '^.*?(?:لا[[:space:]]*(?:اقصد|أقصد|قصدي|اعني|أعني)|مش[[:space:]]*(?:اقصد|أقصد|قصدي|اعني|أعني)|مو[[:space:]]*(?:اقصد|أقصد|قصدي|اعني|أعني)|ما[[:space:]]*(?:اقصد|أقصد|قصدي|اعني|أعني)|(?:انا|أنا)[[:space:]]*(?:اقصد|أقصد|قصدي|اعني|أعني)|i[[:space:]]+mean|no[[:space:]]+i[[:space:]]+mean|not[[:space:]]+(?:dr\.?|doctor)|rather|instead|actually|la2?[[:space:]]+(?:a2sed|ased|asdy|2asdy)|mesh[[:space:]]+(?:a2sed|ased|asdy|2asdy)|msh[[:space:]]+(?:a2sed|ased|asdy|2asdy)|2asdy|asdy)[[:space:]]*', '', 'i'
            ),
            '^(?:الدكتورة|دكتوره|طبيبة|طبيبه|الدكتور|دكتور|د\.|الطبيب|طبيب|dr\.?|doctor)[[:space:]]*', '', 'i'
          ),
          '[[:space:]]+(?:بل|قصدي|يعني|but|rather|instead).*$|[؟?!،,.].*$', '', 'i'
        )), '')
      ELSE NULL
    END AS correction_name
), named AS (
  SELECT
    e.*,
    (NULLIF(TRIM(COALESCE(e.correction_name, '')), '') IS NOT NULL) AS correction_detected,
    COALESCE(
      NULLIF(TRIM(e.correction_name), ''),
      NULLIF(TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(e.message_text, '^.*?(?:الدكتورة|دكتوره|طبيبة|طبيبه|الدكتور|دكتور|د\.|الطبيب|طبيب|dr\.?|doctor)[[:space:]]*', '', 'i'),
          '[[:space:]]*(?:موجود(?:ة)?|متاح(?:ة)?|متوفر(?:ة)?|available|there|يوم|بكرة|بكره|بعد[[:space:]]+بكرة|بعد[[:space:]]+بكره|الاحد|الأحد|الاثنين|الثلاثاء|الاربع|الأربع|الاربعاء|الأربعاء|الخميس|الجمعة|الجمعه|السبت|الساعة|الساعه|عند).*$','', 'i'
        ),
        '[؟?!،,.].*$', '', 'i'
      )), '' )
    ) AS requested_doctor_name
  FROM extracted e
), matched AS (
  SELECT
    e.is_doctor_inquiry,
    e.is_doctor_catalog_inquiry,
    e.requested_doctor_name,
    e.correction_detected,
    CASE WHEN e.correction_detected THEN 'doctor_entity_correction' ELSE NULL END AS correction_type,
    d.id AS doctor_id,
    d.name AS doctor_name
  FROM named e
  LEFT JOIN doctors d
    ON d.clinic_id = e.clinic_id
   AND d.is_active = true
   AND d.deleted_at IS NULL
   AND e.is_doctor_inquiry = true
   AND e.requested_doctor_name IS NOT NULL
   AND REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(d.name, '[أإآٱ]', 'ا', 'g'), 'ى', 'ي', 'g'), 'ة', 'ه', 'g') ILIKE '%' || REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(e.requested_doctor_name, '[أإآٱ]', 'ا', 'g'), 'ى', 'ي', 'g'), 'ة', 'ه', 'g') || '%'
), catalog_doctors AS (
  SELECT
    e.is_doctor_catalog_inquiry,
    d.id AS doctor_id,
    d.name AS doctor_name
  FROM named e
  LEFT JOIN doctors d
    ON d.clinic_id = e.clinic_id
   AND d.is_active = true
   AND d.deleted_at IS NULL
  WHERE e.is_doctor_catalog_inquiry = true
)
SELECT
  is_doctor_inquiry,
  is_doctor_catalog_inquiry,
  requested_doctor_name,
  correction_detected,
  correction_type,
  (COUNT(doctor_id) > 0) AS found,
  (ARRAY_AGG(doctor_id ORDER BY doctor_name) FILTER (WHERE doctor_id IS NOT NULL))[1] AS doctor_id,
  (ARRAY_AGG(doctor_name ORDER BY doctor_name) FILTER (WHERE doctor_name IS NOT NULL))[1] AS doctor_name,
  COALESCE(
    JSONB_AGG(JSONB_BUILD_OBJECT('doctor_id', doctor_id, 'doctor_name', doctor_name) ORDER BY doctor_name)
      FILTER (WHERE doctor_id IS NOT NULL),
    '[]'::jsonb
  ) AS matches,
  COALESCE((
    SELECT JSONB_AGG(JSONB_BUILD_OBJECT('doctor_id', c.doctor_id, 'doctor_name', c.doctor_name) ORDER BY c.doctor_name)
    FROM catalog_doctors c
    WHERE c.is_doctor_catalog_inquiry = true
      AND c.doctor_id IS NOT NULL
  ), '[]'::jsonb) AS catalog
FROM matched
GROUP BY is_doctor_inquiry, is_doctor_catalog_inquiry, requested_doctor_name, correction_detected, correction_type;'''

# Resolve Service Fact (Deterministic) (extracted/sql/Resolve_Service_Fact_Deterministic.json)
QUERY_RESOLVE_SERVICE_FACT_DETERMINISTIC = r'''WITH input AS (
  SELECT
    $1::text AS message_text,
    $2::uuid AS clinic_id,
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(lower(trim($1::text)), '[أإآ]', 'ا', 'g'),
          'ة', 'ه', 'g'
        ),
        '[ًٌٍَُِّْـ]', '', 'g'
      ),
      '[-،,;؛:!؟?()./]+', ' ', 'g'
    ) AS normalized_message
), flags AS (
  SELECT *,
    (
      normalized_message ~
      '(سعر(ه|ها|هم|هن)?|اسعار(ه|ها|هم|هن)?|تكلف(ه|ته|تها|هم|ها)?|رسوم(ه|ها|هم|هن)?|قيم(ه|ته|تها)?|بكم|كم[[:space:]]+(يكلف|تكلف|تكلفه|سعر|ثمن|الحساب|رسوم)|وش[[:space:]]+(تكلفه|سعر|رسوم))'
    ) AS is_price_inquiry,
    (
      normalized_message ~
      '(الخدمات([[:space:]]+(المتاحه|الموجوده))?|خدمات(كم|هم|ها|هن)?|وش[[:space:]]+(عندكم|الخدمات|خدماتكم|تقدمون)|ايش[[:space:]]+(عندكم|الخدمات|خدماتكم|تقدمون)|ايه[[:space:]]+(الخدمات|عندكم)|شو[[:space:]]+(عندكم|الخدمات|خدماتكم)|ماذا[[:space:]]+تقدمون)'
    ) AS is_service_catalog_inquiry
  FROM input
), service_pool AS (
  SELECT
    s.id AS service_id,
    s.name AS service_name,
    s.price,
    s.duration_minutes,
    s.online_booking,
    s.sort_order,
    f.normalized_message,
    trim(regexp_replace(
      regexp_replace(f.normalized_message, '(^|[[:space:]])ال', ' ', 'g'),
      '[[:space:]]+', ' ', 'g'
    )) AS normalized_message_no_articles,
    trim(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                lower(trim(regexp_replace(s.name, '[[:space:]]*[—–].*$', ''))),
                '[أإآ]', 'ا', 'g'
              ),
              'ة', 'ه', 'g'
            ),
            '[ًٌٍَُِّْـ]', '', 'g'
          ),
          '[-،,;؛:!؟?()./]+', ' ', 'g'
        ),
        '(^|[[:space:]])ال', ' ', 'g'
      )
    ) AS normalized_service_name,
    f.clinic_id,
    f.is_price_inquiry,
    f.is_service_catalog_inquiry
  FROM public.services s
  CROSS JOIN flags f
  WHERE s.clinic_id = f.clinic_id
    AND (f.is_price_inquiry OR f.is_service_catalog_inquiry)
    AND s.is_active = true
    AND s.deleted_at IS NULL
    -- Test fixtures are internal only and must never be customer-facing facts.
    AND s.name !~* '(^|[[:space:]_-])(k2[[:space:]_-]*test|reliability[[:space:]_-]*service|reliability-create-phase|e2e[-_ ]?owned[-_ ]?create)'
), service_terms AS (
  SELECT
    p.service_id,
    COALESCE(
      array_agg(DISTINCT token) FILTER (
        WHERE length(token) >= 3
          AND token !~ '^(خدمه|خدمات|عياده|ال)$'
      ),
      ARRAY[]::text[]
    ) AS significant_terms
  FROM service_pool p
  CROSS JOIN LATERAL regexp_split_to_table(p.normalized_service_name, '[[:space:]]+') AS token
  GROUP BY p.service_id
), term_frequency AS (
  -- Generic terms such as «اسنان» occur in many names and carry less evidence
  -- than specific terms such as «تقويم» or «جذور».
  SELECT term, count(DISTINCT service_id) AS services_with_term
  FROM service_terms
  CROSS JOIN LATERAL unnest(significant_terms) AS term
  GROUP BY term
), match_signals AS (
  SELECT
    p.*,
    t.significant_terms,
    position(
      ' ' || p.normalized_service_name || ' '
      IN ' ' || p.normalized_message_no_articles || ' '
    ) > 0 AS full_name_match,
    COALESCE((
      SELECT sum(1.0 / tf.services_with_term)
      FROM unnest(t.significant_terms) AS term
      JOIN term_frequency tf USING (term)
      WHERE position(
        ' ' || term || ' '
        IN ' ' || p.normalized_message_no_articles || ' '
      ) > 0
    ), 0) AS term_match_score,
    (SELECT count(*) FROM service_pool) AS service_count
  FROM service_pool p
  JOIN service_terms t USING (service_id)
), candidate_matches AS (
  SELECT *,
    (CASE WHEN full_name_match THEN 100 ELSE 0 END) + term_match_score AS match_score
  FROM match_signals
  WHERE is_price_inquiry
    AND (
      full_name_match
      OR term_match_score > 0
      OR service_count = 1
    )
), matched AS (
  -- Keep every best match. A patient-facing layer will ask for clarification
  -- rather than silently selecting one service and one price.
  SELECT *
  FROM candidate_matches
  WHERE match_score = (SELECT max(match_score) FROM candidate_matches)
), agg AS (
  SELECT
    (SELECT is_price_inquiry OR is_service_catalog_inquiry FROM flags) AS is_service_fact_inquiry,
    (SELECT is_price_inquiry FROM flags) AS is_price_inquiry,
    (SELECT is_service_catalog_inquiry FROM flags) AS is_service_catalog_inquiry,
    CASE
      WHEN (SELECT is_service_catalog_inquiry FROM flags)
        AND NOT (SELECT is_price_inquiry FROM flags)
        THEN EXISTS (SELECT 1 FROM service_pool)
      ELSE EXISTS (SELECT 1 FROM matched)
    END AS found,
    (SELECT count(*) FROM matched) AS match_count,
    CASE WHEN (SELECT count(*) FROM matched) = 1
      THEN (SELECT service_id FROM matched LIMIT 1)
      ELSE NULL
    END AS service_id,
    CASE WHEN (SELECT count(*) FROM matched) = 1
      THEN (SELECT service_name FROM matched LIMIT 1)
      ELSE NULL
    END AS service_name,
    CASE WHEN (SELECT count(*) FROM matched) = 1
      THEN (SELECT price FROM matched LIMIT 1)
      ELSE NULL
    END AS price,
    CASE WHEN (SELECT count(*) FROM matched) = 1
      THEN (SELECT duration_minutes FROM matched LIMIT 1)
      ELSE NULL
    END AS duration_minutes,
    COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'service_id', service_id,
          'service_name', service_name,
          'price', price,
          'duration_minutes', duration_minutes,
          'online_booking', online_booking
        )
        ORDER BY sort_order NULLS LAST, service_name
      )
      FROM matched
    ), '[]'::jsonb) AS matches,
    COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'service_id', service_id,
          'service_name', service_name,
          'price', price,
          'duration_minutes', duration_minutes,
          'online_booking', online_booking
        )
        ORDER BY sort_order NULLS LAST, service_name
      )
      FROM service_pool
    ), '[]'::jsonb) AS catalog
)
SELECT * FROM agg;'''

# Lookup Business Time Context (extracted/sql/Lookup_Business_Time_Context.json)
QUERY_LOOKUP_BUSINESS_TIME_CONTEXT = r'''WITH request AS (
  SELECT $1::uuid AS clinic_id, NULLIF($2::text, '')::uuid AS slot_id
), clinic AS (
  SELECT c.id AS clinic_id, NULLIF(btrim(c.timezone), '') AS timezone
  FROM public.clinics c
  JOIN request r ON r.clinic_id = c.id
  WHERE c.deleted_at IS NULL
  LIMIT 1
), slot AS (
  SELECT s.id::text AS slot_id, s.start_time, s.end_time, s.clinic_id
  FROM public.appointment_slots s
  JOIN request r ON r.slot_id = s.id
  WHERE s.clinic_id = r.clinic_id
    AND s.deleted_at IS NULL
  LIMIT 1
), hours AS (
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'day_of_week', h.day_of_week,
        'open_time', h.open_time::text,
        'close_time', h.close_time::text,
        'is_off_day', h.is_off_day
      ) ORDER BY h.day_of_week, h.open_time
    ),
    '[]'::jsonb
  ) AS business_hours
  FROM public.clinic_business_hours h
  JOIN request r ON r.clinic_id = h.clinic_id
  WHERE h.deleted_at IS NULL
)
SELECT
  r.clinic_id,
  r.slot_id,
  (s.slot_id IS NOT NULL) AS slot_found,
  s.start_time,
  s.end_time,
  c.timezone,
  (c.timezone IS NOT NULL AND EXISTS (
    SELECT 1 FROM pg_timezone_names tz WHERE tz.name = c.timezone
  )) AS timezone_configured,
  CASE
    WHEN c.timezone IS NULL THEN 'CLINIC_TIMEZONE_NOT_CONFIGURED'
    WHEN NOT EXISTS (SELECT 1 FROM pg_timezone_names tz WHERE tz.name = c.timezone) THEN 'CLINIC_TIMEZONE_INVALID'
    ELSE NULL
  END AS timezone_error_code,
  h.business_hours
FROM request r
LEFT JOIN clinic c ON c.clinic_id = r.clinic_id
LEFT JOIN slot s ON s.clinic_id = r.clinic_id
CROSS JOIN hours h
LIMIT 1;'''

# Claim Operation (Atomic) (extracted/sql/Claim_Operation_Atomic.json)
QUERY_CLAIM_OPERATION_ATOMIC = r'''WITH claim AS (
  SELECT c.decision, c.operation_id, c.operation_status, c.mutation_status, c.response_json
  FROM public.flow_up_claim_operation(
    $1::uuid,
    NULLIF($2::text, '')::uuid,
    NULLIF($3::text, '')::uuid,
    NULLIF(NULLIF($4::text, '__NULL__'), ''),
    NULLIF($5::text, '')::uuid,
    NULLIF(NULLIF($6::text, '__NULL__'), '')
  ) c
  LIMIT 1
)
SELECT
  claim.decision,
  claim.operation_id,
  claim.operation_status,
  claim.mutation_status,
  claim.response_json,
  l.created_at AS ledger_created_at,
  CASE WHEN claim.decision = 'INCONCLUSIVE'
        OR (claim.decision = 'IN_PROGRESS' AND l.created_at < now() - interval '10 minutes')
       THEN true ELSE false END AS ledger_alert,
  CASE WHEN l.created_at IS NULL THEN NULL
       ELSE GREATEST(0, EXTRACT(EPOCH FROM (now() - l.created_at)))::integer END AS ledger_age_seconds
FROM claim
LEFT JOIN public.flow_up_operation_ledger l
  ON l.clinic_id = $1::uuid AND l.operation_id = claim.operation_id
LIMIT 1;'''

# Finalize Operation (Atomic) (extracted/sql/Finalize_Operation_Atomic.json)
QUERY_FINALIZE_OPERATION_ATOMIC = r'''SELECT c.operation_id, c.operation_status, c.mutation_status, c.response_json
FROM public.flow_up_complete_operation(
  $1::uuid,
  NULLIF(NULLIF($2::text, '__NULL__'), ''),
  NULLIF(NULLIF($3::text, '__NULL__'), ''),
  NULLIF(NULLIF($4::text, '__NULL__'), ''),
  NULLIF(convert_from(decode(NULLIF($5::text, '__NULL__'), 'base64'), 'UTF8'), '')::jsonb,
  NULLIF(NULLIF($6::text, '__NULL__'), ''),
  NULLIF(convert_from(decode(NULLIF($7::text, '__NULL__'), 'base64'), 'UTF8'), '')::jsonb
) c
LIMIT 1;'''

# Get Conversation State (retry) (v18) (extracted/sql/Get_Conversation_State_retry_v18.json)
QUERY_GET_CONVERSATION_STATE_RETRY_V18 = r'''WITH eligible_states AS (
  SELECT cs.conversation_id, cs.state_data, (cs.conversation_id = $1::uuid) AS is_current
  FROM conversation_state cs
  JOIN conversations c ON c.id = cs.conversation_id
  WHERE c.clinic_id = $2::uuid AND c.patient_id = $3::uuid AND c.deleted_at IS NULL
), current_state AS (
  SELECT state_data FROM eligible_states WHERE is_current LIMIT 1
), merged_facts AS (
  SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb) AS facts
  FROM (
    SELECT key, value, ROW_NUMBER() OVER (PARTITION BY key ORDER BY is_current DESC) AS rn
    FROM eligible_states, LATERAL jsonb_each(COALESCE(state_data->'facts', '{}'::jsonb))
    WHERE key IN ('clinic', 'patient')
  ) fact_rows
  WHERE rn = 1
), raw_state AS (
  SELECT COALESCE((SELECT state_data FROM current_state), '{}'::jsonb) || jsonb_build_object(
    'facts', (SELECT facts FROM merged_facts)
  ) AS sd
), flags AS (
  SELECT sd,
    (
      (COALESCE(sd->>'draft_expires_at', '') ~ '^\d{4}-\d{2}-\d{2}T'
        AND (sd->>'draft_expires_at')::timestamptz <= now())
      OR (
        COALESCE(sd->>'operation_state', '') IN ('DRAFT','COLLECTING_DETAILS','COLLECTING_APPOINTMENT_DETAILS')
        AND COALESCE(sd->>'draft_expires_at', '') = ''
        AND COALESCE(sd->>'last_updated', '') ~ '^\d{4}-\d{2}-\d{2}T'
        AND (sd->>'last_updated')::timestamptz <= now() - interval '30 minutes'
      )
    ) AS draft_expired,
    (
      COALESCE(sd#>>'{booking_context,patient_address}', '') ~ '^\{'
      OR COALESCE(sd#>>'{booking_context,patient_address}', '') ~ '^\['
      OR COALESCE(sd#>>'{facts,patient,address}', '') ~ '^\{'
      OR COALESCE(sd#>>'{facts,patient,address}', '') ~ '^\['
      OR length(COALESCE(sd#>>'{booking_context,patient_address}', '')) > 600
    ) AS address_polluted
  FROM raw_state
), scrubbed AS (
  SELECT
    CASE
      WHEN f.draft_expired THEN jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(
        f.sd,
        '{booking_context,date}', 'null'::jsonb, true),
        '{booking_context,time}', 'null'::jsonb, true),
        '{booking_context,slot_id}', 'null'::jsonb, true),
        '{slot_state,date}', 'null'::jsonb, true),
        '{slot_state,time}', 'null'::jsonb, true),
        '{slot_state,slot_id}', 'null'::jsonb, true),
        '{facts,booking,date}', 'null'::jsonb, true),
        '{facts,booking,time}', 'null'::jsonb, true)
      ELSE f.sd
    END AS sd,
    f.draft_expired,
    f.address_polluted
  FROM flags f
), cleaned AS (
  SELECT
    CASE
      WHEN s.draft_expired AND COALESCE(s.sd->>'operation_state', '') IN ('DRAFT','COLLECTING_DETAILS','COLLECTING_APPOINTMENT_DETAILS','COLLECTING_PATIENT_DATA') THEN jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(jsonb_set(
        s.sd,
        '{operation_state}', '"IDLE"'::jsonb, true),
        '{operation_status}', '"idle"'::jsonb, true),
        '{active_operation}', 'null'::jsonb, true),
        '{operation_id}', 'null'::jsonb, true),
        '{routing_action}', 'null'::jsonb, true),
        '{pending_action}', 'null'::jsonb, true),
        '{confirmation_state}', 'null'::jsonb, true),
        '{draft_started_at}', 'null'::jsonb, true),
        '{draft_expires_at}', 'null'::jsonb, true)
      ELSE s.sd
    END AS sd,
    s.address_polluted
  FROM scrubbed s
)
SELECT $1::uuid AS conversation_id,
  CASE
    WHEN c.address_polluted THEN jsonb_set(jsonb_set(jsonb_set(
      c.sd,
      '{booking_context,patient_address}', 'null'::jsonb, true),
      '{facts,patient,address}', 'null'::jsonb, true),
      '{patient_data_review,fields,address}', 'null'::jsonb, true)
    ELSE c.sd
  END AS state_data
FROM cleaned c;'''

# Get Active Handoff Request (extracted/sql/Get_Active_Handoff_Request.json)
QUERY_GET_ACTIVE_HANDOFF_REQUEST = r'''WITH active AS (
  SELECT id, status::text AS status, handoff_reason, priority::text AS priority, requested_at
  FROM public.handoff_requests
  WHERE conversation_id = $1::uuid
    AND clinic_id = $2::uuid
    AND patient_id = $3::uuid
    AND status IN ('REQUESTED', 'OPEN', 'ASSIGNED', 'IN_PROGRESS')
  ORDER BY updated_at DESC
  LIMIT 1
)
SELECT
  (SELECT id FROM active) AS handoff_request_id,
  (SELECT status FROM active) AS handoff_status,
  (SELECT handoff_reason FROM active) AS handoff_reason,
  (SELECT priority FROM active) AS handoff_priority,
  (SELECT requested_at FROM active) AS handoff_requested_at;'''

# Get Recent Window 2h (extracted/sql/Get_Recent_Window_2h.json)
QUERY_GET_RECENT_WINDOW_2H = r'''WITH input AS (
  SELECT
    $1::uuid AS conversation_id,
    $2::uuid AS clinic_id,
    $3::uuid AS patient_id,
    $4::timestamptz AS reference_at,
    $5::text AS message_text
), flags AS (
  SELECT *, (message_text ~* '(الحجز السابق|الحجز القديم|الموعد السابق|الموعد القديم|آخر حجز|اخر حجز|آخر موعد|اخر موعد|رقم الحجز|رقم التذكرة|التذكرة السابقة|التذكرة القديمة|كنت حاجز|كنت حاجزة|كنت حاجز قبل كده|كنت حاجزة قبل كده|انا كنت حاجز|انا كنت حاجزة|حجزت قبل|حجزت من قبل|حجزت قبل كده|قلت لك قبل|قلت لكم قبل|اللي قلته قبل|الموعد اللي قلت لك عليه قبل|الموعد اللي قلت لك عنه قبل|اللي قلت لك عليه قبل|اللي قلت لك عنه قبل|الموعد اللي ذكرته قبل|الموعد اللي تكلمنا عنه قبل|الحجز بتاعي|الحجز بتاعى|الموعد بتاعي|الموعد بتاعى|المعاد بتاعي|المعاد بتاعى|الحجز اللي فات|الحجز اللى فات|الموعد اللي فات|الموعد اللى فات|المعاد اللي فات|المعاد اللى فات|اللي فات|اللى فات|اللي قبل كده|اللى قبل كده|سابقًا|سابقا|الماضي|previous booking|past booking|old booking|earlier booking|previous appointment|last appointment|my booking|my appointment|my ticket|الحجز اللي عملته|الموعد اللي حجزته|حجزتي السابقة|حجزتي قبل|موعدي السابق|موعدي القديم|الميعاد اللي فات|اللي قبل|اللي فات|من قبل|قبل كده|قبل كدا|القديم بتاعي|the appointment I had|the one before|from before|my old appointment|last one)') AS historical_reference_requested
  FROM input
), windowed AS (
  SELECT
    m.id,
    m.sender_type,
    m.direction,
    m.content,
    m.received_at,
    m.message_status,
    f.historical_reference_requested
  FROM public.messages m
  CROSS JOIN flags f
  WHERE m.conversation_id = f.conversation_id
    AND m.clinic_id = f.clinic_id
    AND m.patient_id = f.patient_id
    AND m.deleted_at IS NULL
    AND COALESCE(m.metadata->>'k2_deferred_replay', 'false') <> 'true'
    AND ((NOT f.historical_reference_requested AND m.received_at >= f.reference_at - interval '2 hours') OR f.historical_reference_requested)
    AND m.received_at <= f.reference_at
  ORDER BY m.received_at DESC, m.id DESC
  LIMIT CASE WHEN (SELECT historical_reference_requested FROM flags) THEN 12 ELSE 6 END
), history AS (
  SELECT * FROM windowed ORDER BY received_at ASC, id ASC
)
SELECT jsonb_build_object(
  'schema_version', 4,
  'history_scope', CASE WHEN i.historical_reference_requested THEN 'explicit_historical_reference_capped12' ELSE 'window_2h_capped6' END,
  'window_hours', CASE WHEN i.historical_reference_requested THEN NULL ELSE 2 END,
  'window_message_cap', CASE WHEN i.historical_reference_requested THEN 12 ELSE 6 END,
  'historical_reference_requested', i.historical_reference_requested,
  'reference_at', i.reference_at,
  'history_source', 'public.messages',
  'total_messages', (SELECT COUNT(*)::integer FROM history),
  'recent_window_2h', COALESCE((
    SELECT jsonb_agg(jsonb_build_object(
      'message_id', h.id,
      'sender_type', h.sender_type,
      'direction', h.direction,
      'content', h.content,
      'received_at', h.received_at,
      'message_status', h.message_status
    ) ORDER BY h.received_at ASC, h.id ASC)
    FROM history h
  ), '[]'::jsonb)
) AS conversation_history
FROM flags i;'''

# Persist Pending Confirmation (Deterministic) (extracted/sql/Persist_Pending_Confirmation_Deterministic.json)
QUERY_PERSIST_PENDING_CONFIRMATION_DETERMINISTIC = r'''INSERT INTO public.flow_up_confirmations (confirmation_id, clinic_id, patient_id, conversation_id, action, target_fingerprint, payload_snapshot, issued_at, expires_at, status) SELECT NULLIF($2::text,'')::uuid, NULLIF($3::text,'')::uuid, NULLIF($4::text,'')::uuid, NULLIF($5::text,'')::uuid, NULLIF($6::text,''), encode(extensions.digest(convert_to(COALESCE(NULLIF($7::text,''), NULLIF($8::text,''), ''),'UTF8'),'sha256'),'hex'), COALESCE(NULLIF($8::text,'')::jsonb, '{}'::jsonb), now(), now() + make_interval(secs => $9::int), 'PENDING' WHERE upper($1::text) = 'CONFIRMATION_REQUIRED' ON CONFLICT (confirmation_id) DO UPDATE SET clinic_id=EXCLUDED.clinic_id, patient_id=EXCLUDED.patient_id, conversation_id=EXCLUDED.conversation_id, action=EXCLUDED.action, target_fingerprint=EXCLUDED.target_fingerprint, payload_snapshot=EXCLUDED.payload_snapshot, expires_at=EXCLUDED.expires_at, status='PENDING', consumed_at=NULL WHERE public.flow_up_confirmations.status <> 'CONSUMED' RETURNING confirmation_id, status;'''

# Insert AI Request Usage (extracted/sql/Insert_AI_Request_Usage.json)
QUERY_INSERT_AI_REQUEST_USAGE = r'''INSERT INTO public.ai_requests (
  clinic_id, conversation_id, provider, model, input_tokens, output_tokens, total_tokens,
  cost, response_received_at, metadata, request_payload
) VALUES (
  NULLIF($1::text,'null')::uuid,
  NULLIF($2::text,'null')::uuid,
  $3,
  $4,
  NULLIF($5::text,'null')::integer,
  NULLIF($6::text,'null')::integer,
  NULLIF($7::text,'null')::integer,
  NULLIF($8::text,'null')::numeric,
  NULLIF($9::text,'null')::timestamptz,
  COALESCE(NULLIF($10::text,'null'),'{}')::jsonb,
  COALESCE(NULLIF($11::text,'null'),'{}')::jsonb
);'''

# Resolve Branch Inquiry (Deterministic) (extracted/sql/Resolve_Branch_Inquiry_Deterministic.json)
QUERY_RESOLVE_BRANCH_INQUIRY_DETERMINISTIC = r'''WITH extracted AS (
  SELECT
    $1::uuid AS clinic_id,
    $2::text AS message_text,
    CASE
      WHEN $2::text ~* '(?:فروعكم|الفروع|فروع[[:space:]]+(?:العياده|العيادة|المتاحه|الموجوده)|كام[[:space:]]+فرع|عدد[[:space:]]+الفروع|اماكنكم|امكنة[[:space:]]+العياده|مواقع(?:كم)?|branches|locations|which[[:space:]]+locations)'
      THEN true ELSE false
    END AS is_branch_catalog_inquiry,
    CASE
      WHEN $2::text ~* '(?:^|[[:space:]])(?:فرع|عنوان|مكان|لوكيشن|وين|فين)[[:space:]]+[^؟?!،,.]{2,60}'
        OR $2::text ~* '(?:^|[[:space:]])(?:where[[:space:]]+is|address[[:space:]]+of|location[[:space:]]+of)[[:space:]]+[^?!,.]{2,60}'
      THEN true ELSE false
    END AS is_branch_inquiry,
    NULLIF(TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE($2::text, '^.*?(?:فرع|عنوان|مكان|لوكيشن|وين|فين)[[:space:]]*', '', 'i'),
      '[؟?!،,.].*$', '', 'i'
    )), '') AS requested_branch_name
), matched AS (
  SELECT
    e.is_branch_inquiry,
    e.is_branch_catalog_inquiry,
    e.requested_branch_name,
    b.id AS branch_id,
    b.name AS branch_name
  FROM extracted e
  LEFT JOIN branches b
    ON b.clinic_id = e.clinic_id
   AND b.is_active = true
   AND b.deleted_at IS NULL
   AND e.is_branch_inquiry = true
   AND e.requested_branch_name IS NOT NULL
   AND REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(b.name, '[أإآٱ]', 'ا', 'g'), 'ى', 'ي', 'g'), 'ة', 'ه', 'g') ILIKE '%' || REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(e.requested_branch_name, '[أإآٱ]', 'ا', 'g'), 'ى', 'ي', 'g'), 'ة', 'ه', 'g') || '%'
), catalog_branches AS (
  SELECT
    e.is_branch_catalog_inquiry,
    b.id AS branch_id,
    b.name AS branch_name
  FROM extracted e
  LEFT JOIN branches b
    ON b.clinic_id = e.clinic_id
   AND b.is_active = true
   AND b.deleted_at IS NULL
  WHERE e.is_branch_catalog_inquiry = true
)
SELECT
  is_branch_inquiry,
  is_branch_catalog_inquiry,
  requested_branch_name,
  (COUNT(branch_id) > 0) AS found,
  (ARRAY_AGG(branch_id ORDER BY branch_name) FILTER (WHERE branch_id IS NOT NULL))[1] AS branch_id,
  (ARRAY_AGG(branch_name ORDER BY branch_name) FILTER (WHERE branch_id IS NOT NULL))[1] AS branch_name,
  COALESCE(
    JSONB_AGG(JSONB_BUILD_OBJECT('branch_id', branch_id, 'branch_name', branch_name) ORDER BY branch_name)
      FILTER (WHERE branch_id IS NOT NULL),
    '[]'::jsonb
  ) AS matches,
  COALESCE((
    SELECT JSONB_AGG(JSONB_BUILD_OBJECT('branch_id', c.branch_id, 'branch_name', c.branch_name) ORDER BY c.branch_name)
    FROM catalog_branches c
    WHERE c.is_branch_catalog_inquiry = true
      AND c.branch_id IS NOT NULL
  ), '[]'::jsonb) AS catalog
FROM matched
GROUP BY is_branch_inquiry, is_branch_catalog_inquiry, requested_branch_name;'''

# K2 Inbound Burst Rate Gate (extracted/sql/K2_Inbound_Burst_Rate_Gate.json)
QUERY_K2_INBOUND_BURST_RATE_GATE = r'''WITH input AS (
  SELECT $1::uuid AS clinic_id, $2::uuid AS patient_id, $3::uuid AS conversation_id,
         $4::text AS message_text, $5::boolean AS deferred_replay
), counts AS (
  SELECT
    (SELECT count(*) FROM public.messages m, input i
     WHERE m.conversation_id = i.conversation_id
       AND m.clinic_id = i.clinic_id
       AND m.direction = 'incoming'
       AND m.sender_type = 'patient'
       AND m.created_at >= now() - INTERVAL '15 seconds')::integer AS conversation_count,
    (SELECT count(*) FROM public.messages m, input i
     WHERE m.clinic_id = i.clinic_id
       AND m.direction = 'incoming'
       AND m.sender_type = 'patient'
       AND m.created_at >= now() - INTERVAL '15 seconds')::integer AS clinic_count
), flags AS (
  SELECT (message_text ~* '(تأكيد|أكد|موافق|الغاء|إلغاء|الغيه|ألغ|تعديل|عدل|غير الموعد|تغيير الموعد|reschedule|cancel|confirm|approve|modify)') AS priority_allow,
         deferred_replay
  FROM input
), decision AS (
  SELECT (
    f.deferred_replay
    OR f.priority_allow
    OR (c.conversation_count <= 8 AND c.clinic_count <= 300)
  ) AS allowed,
  f.priority_allow,
  c.conversation_count,
  c.clinic_count,
  f.deferred_replay
  FROM counts c CROSS JOIN flags f
)
SELECT
  d.allowed,
  d.priority_allow,
  d.conversation_count::integer AS recent_conversation_count,
  d.clinic_count::integer AS recent_clinic_count,
  8::integer AS conversation_limit,
  300::integer AS clinic_limit,
  15::integer AS window_seconds,
  (NOT d.allowed) AS rate_limited,
  d.deferred_replay
FROM decision d
LIMIT 1;'''

# Mark K2 Burst Message Deferred (extracted/sql/Mark_K2_Burst_Message_Deferred.json)
QUERY_MARK_K2_BURST_MESSAGE_DEFERRED = r'''WITH marked AS (
  UPDATE public.messages
  SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('k2_deferred', true, 'k2_deferred_at', now(), 'k2_deferred_status', 'pending')
  WHERE id = $6::uuid AND clinic_id = $1::uuid AND patient_id = $2::uuid AND conversation_id = $3::uuid
  RETURNING id
), enqueued AS (
  SELECT * FROM public.k2_enqueue_deferred_batch_v2($1::uuid, $2::uuid, $3::uuid, $4::text, $5::text, $6::uuid, $7::text, $8::timestamptz, $9::text, $10::boolean)
)
SELECT e.batch_id, e.batch_status, e.batch_message_count, e.batch_due_at,
       EXISTS (SELECT 1 FROM marked) AS message_marked,
       $10::boolean AS priority_allow
FROM enqueued e LIMIT 1;'''

# Log K2 Rate Decision (extracted/sql/Log_K2_Rate_Decision.json)
QUERY_LOG_K2_RATE_DECISION = r'''SELECT public.k2_log_operational_event(
  p_event_type := 'rate_gate',
  p_clinic_id := $1::uuid,
  p_patient_id := $2::uuid,
  p_conversation_id := $3::uuid,
  p_status := CASE WHEN $4::boolean THEN 'allowed' ELSE 'deferred' END,
  p_decision := CASE WHEN $5::boolean THEN 'priority_allow' ELSE CASE WHEN $4::boolean THEN 'allow' ELSE 'defer' END END,
  p_metadata := jsonb_build_object('recent_conversation_count', $6::integer, 'recent_clinic_count', $7::integer, 'conversation_limit', $8::integer, 'clinic_limit', $9::integer, 'window_seconds', $10::integer)
);'''

# Verify K2 Inbound Signature (extracted/sql/Verify_K2_Inbound_Signature.json)
QUERY_VERIFY_K2_INBOUND_SIGNATURE = r'''SELECT * FROM public.k2_verify_inbound_signature($1::uuid, $2::text, $3::text, $4::text, $5::text, $6::boolean) LIMIT 1;'''

# Read Fresh Offer (Midturn) (extracted/sql/Read_Fresh_Offer_Midturn.json)
QUERY_READ_FRESH_OFFER_MIDTURN = r'''SELECT cs.state_data->'presented_offer' AS presented_offer,
       COALESCE(cs.state_data->'presented_offer'->'alternatives', '[]'::jsonb) AS availability_alternatives
FROM conversation_state cs
JOIN conversations c ON c.id = cs.conversation_id AND c.clinic_id = $2::uuid AND c.patient_id = $3::uuid AND c.deleted_at IS NULL
WHERE cs.conversation_id = $1::uuid
LIMIT 1;'''

# Port-added: direct Postgres call replacing the Supabase REST RPC used by the
# n8n HTTP nodes Save Conversation State / Save Conversation State (retry) (v18).
# LIMIT 1 mirrors PostgREST returning a single JSON envelope; the repository
# unwraps a scalar-JSON return to its document form (PostgREST behavior).
QUERY_SAVE_CONVERSATION_STATE_RPC = r'''
SELECT * FROM public.k2_save_conversation_state($1::uuid, $2::jsonb, $3::integer) LIMIT 1
'''

# Port-added (2026-09-19): double-booking guard. A create that committed but whose
# process died before the state save leaves the conversation at AWAIT_CONFIRMATION;
# the patient's re-affirm mints a NEW operation_id the claim ledger never saw, so
# only this slot-scoped check refuses the second booking. Mirrors the 'scheduled'
# status the create executor books with and the appointments columns the resolver reads.
QUERY_FIND_ACTIVE_APPOINTMENT_FOR_SLOT = r'''SELECT
  a.id,
  a.public_id,
  a.booking_number
FROM public.appointments a
WHERE a.clinic_id = $1::uuid
  AND a.patient_id = $2::uuid
  AND a.slot_id = NULLIF($3::text, '')::uuid
  AND a.deleted_at IS NULL
  AND a.appointment_status = 'scheduled'
LIMIT 1;'''
