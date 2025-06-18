import pytest
import json
import os
from src.unthread_extractor.storage import DuckDBStorage

@pytest.fixture
def temp_db():
    """Fixture for temporary database"""
    db_path = ":memory:"  # Use in-memory database for tests
    storage = DuckDBStorage(db_path)
    yield storage
    storage.close()

def test_create_tables(temp_db):
    """Test table creation"""
    # Verify tables exist by querying them
    tables = temp_db.conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
    """).fetchall()
    
    table_names = [table[0] for table in tables]
    assert "users" in table_names
    assert "conversations" in table_names
    assert "customers" in table_names
    assert "messages" in table_names

def test_store_users(temp_db):
    """Test storing users"""
    test_users = [
        {"id": "user1", "name": "Test User 1"},
        {"id": "user2", "name": "Test User 2"}
    ]
    
    temp_db.store_users(test_users)
    
    # Verify stored data
    stored_users = temp_db.conn.execute("SELECT * FROM users").fetchall()
    assert len(stored_users) == 2
    
    # Verify data content
    for stored_user in stored_users:
        user_data = json.loads(stored_user[1])
        assert user_data["id"] in ["user1", "user2"]
        assert user_data["name"] in ["Test User 1", "Test User 2"]

def test_store_customers(temp_db):
    """Test storing customers"""
    test_customers = [
        {"id": "cust1", "name": "Test Customer 1"},
        {"id": "cust2", "name": "Test Customer 2"}
    ]
    
    temp_db.store_customers(test_customers)
    
    # Verify stored data
    stored_customers = temp_db.conn.execute("SELECT * FROM customers").fetchall()
    assert len(stored_customers) == 2
    
    # Verify data content
    for stored_customer in stored_customers:
        customer_data = json.loads(stored_customer[1])
        assert customer_data["id"] in ["cust1", "cust2"]
        assert customer_data["name"] in ["Test Customer 1", "Test Customer 2"]

def test_store_conversations(temp_db):
    """Test storing conversations"""
    test_conversations = [
        {"id": "conv1", "title": "Test Conversation 1"},
        {"id": "conv2", "title": "Test Conversation 2"}
    ]
    
    temp_db.store_conversations(test_conversations)
    
    # Verify stored data
    stored_conversations = temp_db.conn.execute("SELECT * FROM conversations").fetchall()
    assert len(stored_conversations) == 2
    
    # Verify data content
    for stored_conversation in stored_conversations:
        conversation_data = json.loads(stored_conversation[1])
        assert conversation_data["id"] in ["conv1", "conv2"]
        assert conversation_data["title"] in ["Test Conversation 1", "Test Conversation 2"]

def test_store_messages(temp_db):
    """Test storing messages"""
    test_messages = [
        {"id": "msg1", "content": "Test Message 1"},
        {"id": "msg2", "content": "Test Message 2"}
    ]
    
    temp_db.store_messages(test_messages)
    
    # Verify stored data
    stored_messages = temp_db.conn.execute("SELECT * FROM messages").fetchall()
    assert len(stored_messages) == 2
    
    # Verify data content
    for stored_message in stored_messages:
        message_data = json.loads(stored_message[1])
        assert message_data["id"] in ["msg1", "msg2"]
        assert message_data["content"] in ["Test Message 1", "Test Message 2"]

def test_upsert_behavior(temp_db):
    """Test that storing the same ID updates the record"""
    # Store initial data
    test_user = {"id": "user1", "name": "Original Name"}
    temp_db.store_users([test_user])
    
    # Update the same user
    updated_user = {"id": "user1", "name": "Updated Name"}
    temp_db.store_users([updated_user])
    
    # Verify only one record exists with updated data
    stored_users = temp_db.conn.execute("SELECT * FROM users").fetchall()
    assert len(stored_users) == 1
    user_data = json.loads(stored_users[0][1])
    assert user_data["name"] == "Updated Name"

def test_file_storage():
    """Test storage with actual file"""
    db_path = "test_data/test.db"
    try:
        storage = DuckDBStorage(db_path)
        assert os.path.exists(db_path)
        storage.close()
    finally:
        if os.path.exists(db_path):
            os.remove(db_path)
        if os.path.exists("test_data"):
            os.rmdir("test_data") 