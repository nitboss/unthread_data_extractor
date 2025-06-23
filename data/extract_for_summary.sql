-- COMBINED CONVERSATIONS AND MESSAGES
WITH escalated_conversations AS (
  SELECT 
    data->>'conversationId' AS conversationId,
    MAX(
      STRPOS(data->>'text', 'langchain.slack.com') > 0
      OR (
        LOWER(COALESCE(NULLIF(data->'user'->>'email', ''), data->'sentByUser'->>'email')) LIKE '%@langchain.dev'
        AND LOWER(COALESCE(NULLIF(data->'user'->>'email', ''), data->'sentByUser'->>'email')) NOT IN (
          'chad@langchain.dev', 'crystal@langchain.dev', 'nithin@langchain.dev'
        )
      )
    ) AS internalEscalation
  FROM messages
  GROUP BY data->>'conversationId'
),
message_texts AS (
  SELECT 
    data->>'conversationId' AS conversationId,
    STRING_AGG(data->>'text', '\n<Next_Message>:\n' ORDER BY data->>'timestamp') AS allMessages
  FROM messages
  WHERE typeof(data->>'text') = 'VARCHAR'
    AND TRIM(CAST(data->>'text' AS VARCHAR)) <> ''
    AND CAST(data->>'text' AS VARCHAR) <> '""'
    AND CAST(data->>'text' AS VARCHAR) IS NOT NULL
  GROUP BY data->>'conversationId'
),
classifications AS (
  SELECT 
    conversation_id AS conversationId,
    category,
    sub_category,
    reasoning,
    resolution
  FROM conversation_classifications
)
SELECT 
  c.id,
  c.data->>'sourceType' AS sourceType,
  CASE 
    WHEN c.data->>'status' = 'in_progress' THEN 'pending_customer'
    WHEN c.data->>'status' = 'on_hold' THEN 'pending_engineering'
    ELSE c.data->>'status'
  END AS status,
  COALESCE(c.data->'customer'->'tags'[0]->>'name', 'PAYG') AS category,
  COALESCE(NULLIF(c.data->'assignedToUser'->>'name', ''), 'unassigned') AS assignedTo,
  date_trunc('day', CAST(c.data->>'createdAt' AS date)) AS createdAt,
  date_trunc('day', CAST(c.data->>'updatedAt' AS date)) AS updatedAt,
  date_trunc('day', CAST(c.data->>'closedAt' AS date)) AS closedAt,
  c.data->>'responseTime' AS responseTime,
  c.data->>'resolutionTime' AS resolutionTime,
  c.data->>'waitingOnAgentTime' AS waitingOnAgentTime,
  c.data->>'waitingOnCustomerTime' AS waitingOnCustomerTime,
  c.data->>'priority' AS priority,
  c.data->'ticketType'->>'name' AS ticketType,
  c.data->'submitterUser'->>'email' AS submitter,
  c.data->>'title' AS title,
  c.data->>'summary' AS summary,
  c.data->>'sentiment' AS sentiment,
  COALESCE(e.internalEscalation, false) AS internalEscalation, 
  date_trunc('week', CAST(c.data->>'createdAt' AS date)) AS creation_week,
  mt.allMessages,
  cl.category,
  cl.sub_category,
  cl.reasoning,
  cl.resolution
FROM conversations c
LEFT JOIN escalated_conversations e
  ON c.id = e.conversationId
LEFT JOIN message_texts mt
  ON c.id = mt.conversationId
LEFT JOIN classifications cl
  ON c.id = cl.conversationId
WHERE cl.resolution is null
LIMIT 100;