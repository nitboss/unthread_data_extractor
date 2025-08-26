#!/usr/bin/env python3
"""
Script to add ticket emojis to Slack messages for open tickets
"""

import sys
import os
import logging
import argparse
import time
from typing import List, Dict, Any, Optional
import requests

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from unthread_extractor.storage import DuckDBStorage
from unthread_extractor.config import Config

class TicketEmojiAdder:
    """Class to add ticket emojis to Slack messages"""
    
    def __init__(self, slack_token: str, production_mode: str = "test"):
        """Initialize the emoji adder
        
        Args:
            slack_token: Slack user token for API access
            production_mode: If True, only log actions without making API calls
        """
        self.slack_token = slack_token
        self.production_mode = production_mode
        self.base_url = "https://langchain.slack.com/api"
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            force=True
        )
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Ensure immediate output
        for handler in self.logger.handlers:
            handler.flush = lambda: handler.stream.flush() if hasattr(handler, 'stream') else None
        
        if production_mode:
            self.logger.info("Running in PRODUCTION MODE - API calls will be made")
        else:
            self.logger.info("Running in TEST MODE - no actual API calls will be made")
    
    def get_open_slack_tickets(self, storage: DuckDBStorage, batch_size: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get open tickets from Slack source
        
        Args:
            storage: Database storage instance
            batch_size: Number of tickets to retrieve
            offset: Offset for pagination
            
        Returns:
            List of ticket data dictionaries
        """
        query = """
            SELECT 
                c.id as conversation_id,
                json_extract_string(c.data, 'sourceType') as source_type,
                json_extract_string(c.data, 'status') as status,
                json_extract_string(c.data, 'title') as title,
                json_extract_string(c.data, 'summary') as summary,
                json_extract_string(c.data, 'createdAt') as created_at,
                json_extract_string(c.data, 'updatedAt') as updated_at,
                json_extract_string(c.data, 'initialMessageId') as initial_message_id,
                json_extract_string(c.data, 'channelId') as channel_id,
                -- Extract Slack channel ID and timestamp from Slack ticket structure
                json_extract_string(c.data, 'channelId') as slack_channel_id,
                json_extract_string(c.data, '$.initialMessage.ts') as slack_timestamp
            FROM conversations c
            WHERE json_extract_string(c.data, 'sourceType') = 'slack'
                AND json_extract_string(c.data, 'status') != 'closed'
                AND json_extract_string(c.data, 'channelId') IS NOT NULL
                AND json_extract_string(c.data, '$.initialMessage.ts') IS NOT NULL
            ORDER BY json_extract_string(c.data, 'createdAt') DESC
            LIMIT ? OFFSET ?
        """
        
        try:
            results = storage.conn.execute(query, [batch_size, offset]).fetchall()
            tickets = []
            
            for row in results:
                ticket = {
                    'conversation_id': row[0],
                    'source_type': row[1],
                    'status': row[2],
                    'title': row[3],
                    'summary': row[4],
                    'created_at': row[5],
                    'updated_at': row[6],
                    'initial_message_id': row[7],
                    'channel_id': row[8],
                    'slack_channel_id': row[9],  # Same as channel_id
                    'slack_timestamp': row[10]  # Direct from initialMessage.ts
                }
                tickets.append(ticket)
            
            self.logger.info(f"Retrieved {len(tickets)} open Slack tickets (batch_size={batch_size}, offset={offset})")
            return tickets
            
        except Exception as e:
            self.logger.error(f"Error fetching tickets: {str(e)}")
            raise
    
    def generate_slack_link(self, channel_id: str, timestamp: str) -> str:
        """Generate a Slack link to navigate directly to a message
        
        Args:
            channel_id: Slack channel ID
            timestamp: Slack message timestamp
            
        Returns:
            Slack link string
        """
        # Convert timestamp to proper format for Slack link
        # Slack timestamps are in seconds, but we need to ensure proper formatting
        try:
            # If timestamp is a float/string, convert to proper format
            ts = float(timestamp)
            formatted_ts = f"{ts:.6f}".rstrip('0').rstrip('.')
        except (ValueError, TypeError):
            formatted_ts = str(timestamp)
        
        return f"https://langchain.slack.com/app_redirect?channel={channel_id}&message_ts={formatted_ts}"

    def debug_message_reactions(self, channel_id: str, timestamp: str) -> Dict[str, Any]:
        """Debug method to get detailed information about a message's reactions
        
        Args:
            channel_id: Slack channel ID
            timestamp: Slack message timestamp
            
        Returns:
            Dictionary with debug information
        """
        url = f"{self.base_url}/reactions.get"
        
        params = {
            "channel": channel_id,
            "timestamp": timestamp
        }
        
        headers = {
            "Authorization": f"Bearer {self.slack_token}",
            "Content-Type": "application/json"
        }
        
        debug_info = {
            "channel_id": channel_id,
            "timestamp": timestamp,
            "slack_link": self.generate_slack_link(channel_id, timestamp),
            "success": False,
            "message_info": None,
            "reactions": [],
            "error": None
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                debug_info["success"] = data.get("ok", False)
                
                if data.get("ok"):
                    message = data.get("message", {})
                    debug_info["message_info"] = {
                        "text": message.get("text", "")[:100] + "..." if len(message.get("text", "")) > 100 else message.get("text", ""),
                        "user": message.get("user"),
                        "type": message.get("type"),
                        "subtype": message.get("subtype")
                    }
                    debug_info["reactions"] = message.get("reactions", [])
                else:
                    debug_info["error"] = data.get('error', 'Unknown error')
            else:
                debug_info["error"] = f"HTTP {response.status_code}: {response.text}"
                
        except requests.exceptions.RequestException as e:
            debug_info["error"] = str(e)
        
        return debug_info
    
    def has_ticket_emoji(self, channel_id: str, timestamp: str, emoji: str = "ticket") -> bool:
        """Check if a message already has a ticket emoji
        
        Args:
            channel_id: Slack channel ID
            timestamp: Slack message timestamp
            emoji: Emoji name to check for (without colons)
            
        Returns:
            True if emoji exists, False otherwise
        """
        url = f"{self.base_url}/reactions.get"
        
        params = {
            "channel": channel_id,
            "timestamp": timestamp
        }
        
        headers = {
            "Authorization": f"Bearer {self.slack_token}",
            "Content-Type": "application/json"
        }
        
        if not self.production_mode:
            self.logger.info(f"[TEST] Would check if :{emoji}: exists on message {timestamp} in channel {channel_id}")
            # In test mode, simulate that some messages already have emojis (for testing skip logic)
            # Use timestamp as a simple way to simulate some messages having emojis
            import hashlib
            hash_value = int(hashlib.md5(f"{channel_id}{timestamp}".encode()).hexdigest(), 16)
            has_emoji = (hash_value % 10) < 3  # 30% chance of having emoji in test mode
            if has_emoji:
                self.logger.info(f"[TEST] Simulating existing :{emoji}: on message {timestamp}")
            return has_emoji
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("ok"):
                    message = data.get("message", {})
                    reactions = message.get("reactions", [])
                    
                    # Debug: Log all reactions found
                    if reactions:
                        self.logger.debug(f"Found {len(reactions)} reactions on message {timestamp}: {[r.get('name') for r in reactions]}")
                    else:
                        self.logger.debug(f"No reactions found on message {timestamp}")
                    
                    # Check if any reaction has the ticket emoji name
                    for reaction in reactions:
                        if reaction.get("name") == emoji:
                            self.logger.debug(f"Found existing :{emoji}: reaction on message {timestamp}")
                            return True
                    
                    return False
                else:
                    # Don't log errors here as they're expected (missing_scope, etc.)
                    return False
            else:
                # Don't log errors here as they're expected
                return False
                
        except requests.exceptions.RequestException as e:
            # Don't log exceptions here as they're expected
            return False

    def add_ticket_emoji(self, channel_id: str, timestamp: str, emoji: str = "ticket") -> bool:
        """Add a ticket emoji to a Slack message (only if it doesn't already exist)
        
        Args:
            channel_id: Slack channel ID
            timestamp: Slack message timestamp
            emoji: Emoji name (without colons)
            
        Returns:
            True if successful or already exists, False otherwise
        """
        # Generate Slack link for this message
        slack_link = self.generate_slack_link(channel_id, timestamp)
        
        # Check if emoji already exists
        if self.has_ticket_emoji(channel_id, timestamp, emoji):
            self.logger.info(f"⏭️  Skipped (already has :{emoji}:) | 🔗 Slack Link: {slack_link}")
            sys.stdout.flush()  # Ensure immediate output
            return True
        
        url = f"{self.base_url}/reactions.add"
        
        payload = {
            "channel": channel_id,
            "timestamp": timestamp,
            "name": emoji
        }
        
        headers = {
            "Authorization": f"Bearer {self.slack_token}",
            "Content-Type": "application/json"
        }
        
        if not self.production_mode:
            self.logger.info(f"[TEST] Would add :{emoji}: to message {timestamp} in channel {channel_id}")
            self.logger.info(f"🔗 Slack Link: {slack_link}")
            return True
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("ok"):
                    self.logger.info(f"✅ Added :{emoji}: to message | 🔗 Slack Link: {slack_link}")
                    sys.stdout.flush()  # Ensure immediate output
                    
                    # Verify the emoji was actually added by checking again
                    if self.production_mode:
                        time.sleep(1)  # Wait a moment for Slack to process
                        if self.has_ticket_emoji(channel_id, timestamp, emoji):
                            self.logger.debug(f"Verified :{emoji}: was successfully added to message {timestamp}")
                        else:
                            self.logger.warning(f"API said :{emoji}: was added but verification failed for message {timestamp}")
                    
                    return True
                else:
                    error = data.get('error', 'Unknown error')
                    
                    # If the error is "already_reacted", treat it as success (emoji already exists)
                    if error == "already_reacted":
                        self.logger.info(f"⏭️  Skipped (already has :{emoji}:) | 🔗 Slack Link: {slack_link}")
                        sys.stdout.flush()  # Ensure immediate output
                        return True
                    else:
                        self.logger.error(f"❌ Failed to add :{emoji}: to message | 🔗 Slack Link: {slack_link} | Error: {error}")
                        return False
            else:
                self.logger.error(f"❌ Request failed for message | 🔗 Slack Link: {slack_link} | Status: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Request exception for message | 🔗 Slack Link: {slack_link} | Error: {str(e)}")
            return False
    
    def process_tickets(self, storage: DuckDBStorage, batch_size: int = 100, max_tickets: Optional[int] = None) -> Dict[str, Any]:
        """Process tickets and add emojis
        
        Args:
            storage: Database storage instance
            batch_size: Number of tickets to process per batch
            max_tickets: Maximum number of tickets to process (None for all)
            
        Returns:
            Dictionary with processing results
        """
        offset = 0
        total_processed = 0
        total_successful = 0
        total_failed = 0
        processed_links = []  # Store all processed Slack links
        
        while True:
            # Get batch of tickets
            tickets = self.get_open_slack_tickets(storage, batch_size, offset)
            
            if not tickets:
                self.logger.info("No more tickets to process")
                break
            
            self.logger.debug(f"Processing batch of {len(tickets)} tickets (offset={offset})")
            
            for ticket in tickets:
                if max_tickets and total_processed >= max_tickets:
                    self.logger.debug(f"Reached maximum tickets limit ({max_tickets})")
                    break
                
                total_processed += 1
                
                # Check if we have required Slack data
                if not ticket['slack_channel_id'] or not ticket['slack_timestamp']:
                    self.logger.warning(f"Missing Slack data for ticket {ticket['conversation_id']}: channel_id={ticket['slack_channel_id']}, timestamp={ticket['slack_timestamp']}")
                    total_failed += 1
                    continue
                
                # Generate Slack link for this ticket
                slack_link = self.generate_slack_link(ticket['slack_channel_id'], ticket['slack_timestamp'])
                
                # Add ticket emoji
                success = self.add_ticket_emoji(
                    ticket['slack_channel_id'],
                    ticket['slack_timestamp']
                )
                
                # Store link with status for summary
                processed_links.append({
                    'link': slack_link,
                    'success': success,
                    'title': ticket.get('title', 'No title'),
                    'conversation_id': ticket['conversation_id']
                })
                
                if success:
                    total_successful += 1
                else:
                    total_failed += 1
                
                # Add small delay to avoid rate limiting
                if not self.production_mode:
                    time.sleep(0.1)
            
            if max_tickets and total_processed >= max_tickets:
                break
            
            offset += batch_size
            
            # If we got fewer tickets than batch_size, we've reached the end
            if len(tickets) < batch_size:
                break
        
        # Log summary of all processed links
        self.logger.info(f"\n=== Summary ===")
        self.logger.debug(f"Total threads processed: {len(processed_links)}")
        self.logger.info(f"Successful: {total_successful}, Failed: {total_failed}")
        self.logger.debug(f"Success rate: {(total_successful / len(processed_links) * 100) if processed_links else 0:.1f}%")
        
        # Only show detailed list if there are failures or in debug mode
        if total_failed > 0 or self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"\nAll processed threads:")
            for i, link_info in enumerate(processed_links, 1):
                status_emoji = "✅" if link_info['success'] else "❌"
                self.logger.info(f"{i:3d}. {status_emoji} {link_info['link']} | {link_info['title'][:50]}...")
        
        results = {
            'total_processed': total_processed,
            'total_successful': total_successful,
            'total_failed': total_failed,
            'success_rate': (total_successful / total_processed * 100) if total_processed > 0 else 0,
            'processed_links': processed_links
        }
        
        return results

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Add ticket emojis to Slack messages for open tickets")
    parser.add_argument("--mode", type=str, default="test", help="Run in test or production mode")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of tickets to process per batch (default: 50)")
    parser.add_argument("--max-tickets", type=int, help="Maximum number of tickets to process (default: all)")
    parser.add_argument("--db-path", help="Path to DuckDB database (default: from config)")
    
    args = parser.parse_args()
    
    try:
        # Get Slack token from environment
        slack_token = os.getenv('SLACK_TOKEN')
        if not slack_token:
            raise ValueError("SLACK_TOKEN environment variable not set")
        
        # Initialize components
        config = Config.from_env()
        db_path = args.db_path or config.db_path
        storage = DuckDBStorage(db_path)
        
        # Create emoji adder
        emoji_adder = TicketEmojiAdder(slack_token, production_mode=args.mode == "prod")
        
        # Process tickets
        results = emoji_adder.process_tickets(
            storage=storage,
            batch_size=args.batch_size,
            max_tickets=args.max_tickets
        )
        
        # No need for duplicate summary since it's already shown above
        pass
        
        return 0 if results['total_failed'] == 0 else 1
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return 1
    
    finally:
        if 'storage' in locals():
            storage.close()

if __name__ == '__main__':
    exit(main())
