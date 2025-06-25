/* select data.sourceType, 
  data.status, 
  count(*) 
  from conversations 
  group by 1, 2
  order by 1, 3 desc;

select data.customer.tags[0].name, 
  count(*) 
  from conversations 
  group by 1
  order by 2 desc;
*/

---- RAW CONVERSATIONS
select id
  , data.sourceType
  , CASE 
    WHEN data->>'status' = 'in_progress' THEN 'pending_customer'
    WHEN data->>'status' = 'on_hold' THEN 'pending_engineering'
    ELSE data->>'status'
  END AS status
  , COALESCE(data->'customer'->'tags'[0]->>'name', 'PAYG') AS category
  , COALESCE(NULLIF(data->'assignedToUser'->>'name', ''), 'unassigned') AS assignedTo
  , date_trunc('day', CAST(data->>'createdAt' AS date)) as createdAt
  , date_trunc('day', CAST(data->>'updatedAt' AS date)) as updatedAt
  , date_trunc('day', CAST(data->>'closedAt' AS date)) as closedAt
  , data.responseTime
  , data.resolutionTime
  , data.waitingOnAgentTime
  , data.waitingOnCustomerTime
  , data.priority
  , data.ticketType.name as ticketType
  , data.submitterUser.email as submitter
  , data.title
  , data.summary
  , data.sentiment
from conversations;

---- RAW MESSAGES
select id
  , data.conversationId
  , data.timestamp
  , data.updatedAt
  , data.isPrivateNote
  , data.botName
  , COALESCE(NULLIF(data->'user'->>'email', ''), data->'sentByUser'->>'email') AS sender
  , data.text
  , (
    STRPOS(data->>'text', 'langchain.slack.com') > 0
    OR (
      LOWER(COALESCE(NULLIF(data->'user'->>'email', ''), data->'sentByUser'->>'email')) LIKE '%@langchain.dev'
      AND LOWER(COALESCE(NULLIF(data->'user'->>'email', ''), data->'sentByUser'->>'email')) NOT IN ('chad@langchain.dev', 'crystal@langchain.dev', 'nithin@langchain.dev')
    )
  ) AS internalEscalation
from messages
where 1 = 1
  AND data.text IS NOT NULL 
  AND data.text <> '""';

-- COMBINED CONVERSATIONS AND MESSAGES
select id
  , data.conversationId
  , data.timestamp
  , data.updatedAt
  , data.isPrivateNote
  , data.botName
  , COALESCE(NULLIF(data->'user'->>'email', ''), data->'sentByUser'->>'email') AS sender
  , data.text
  , (
    STRPOS(data->>'text', 'langchain.slack.com') > 0
    OR (
      LOWER(COALESCE(NULLIF(data->'user'->>'email', ''), data->'sentByUser'->>'email')) LIKE '%@langchain.dev'
      AND LOWER(COALESCE(NULLIF(data->'user'->>'email', ''), data->'sentByUser'->>'email')) NOT IN ('chad@langchain.dev', 'crystal@langchain.dev', 'nithin@langchain.dev')
    )
  ) AS internalEscalation
from messages
where 1 = 1
  AND data.text IS NOT NULL 
  AND data.text <> '""';

-- STATS for conversations
SELECT 
  category
  , sub_category
  , count(1) AS cases
FROM conversation_classifications
WHERE 1 = 1
GROUP BY
  category
  , sub_category
ORDER BY 1, 3 desc


-- Slack channels
SELECT data.name
  , data.slackChannel.name as slackChannel
  , data.slackChannel.isPrivate
  , data.externalCrmMetadata.id as sfdc_id	
  , data.tags[0].tier
FROM customers;