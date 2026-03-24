import pytest
from fastapi.testclient import TestClient
from app.gateway.api import app, ACTIVE_SYNC_TASKS
from unittest.mock import patch, MagicMock

client = TestClient(app)

def test_sync_task_locking_acquire():
    """
    测试 POST /api/v1/sync 成功加锁
    """
    import time
    table_id = "test.lock.acquire"
    ACTIVE_SYNC_TASKS.clear()
    
    with patch("app.service.sync_manager.SyncManager.sync") as mock_sync:
        # 让同步任务执行一段时间，以便我们可以在执行期间检查锁
        def slow_sync(*args, **kwargs):
            time.sleep(0.1)
            return None
        mock_sync.side_effect = slow_sync
        
        # 使用线程来发送请求，这样我们可以在任务执行期间检查锁
        import threading
        lock_checked = threading.Event()
        
        def send_request():
            response = client.post("/api/v1/sync", json={
                "table_ids": [table_id],
                "formats": ["csv"]
            })
            assert response.status_code == 200
        
        # 启动请求线程
        thread = threading.Thread(target=send_request)
        thread.start()
        
        # 等待一小段时间让任务开始执行
        time.sleep(0.05)
        
        # 检查锁是否已添加
        assert table_id in ACTIVE_SYNC_TASKS, "任务执行期间应该有锁"
        
        # 等待线程完成
        thread.join()

def test_sync_task_locking_conflict():
    """
    测试锁未释放前再次请求相同 table_id 返回 409
    """
    table_id = "test.lock.conflict"
    ACTIVE_SYNC_TASKS.clear()
    
    # 手动添加锁
    ACTIVE_SYNC_TASKS.add(table_id)
    
    response = client.post("/api/v1/sync", json={
        "table_ids": [table_id],
        "formats": ["csv"]
    })
    
    assert response.status_code == 409
    assert "Tasks already running" in response.json()["detail"]
    
    # 清理
    ACTIVE_SYNC_TASKS.clear()

def test_sync_task_locking_release():
    """
    测试同步完成后锁自动释放
    """
    table_id = "test.lock.release"
    ACTIVE_SYNC_TASKS.clear()
    
    with patch("app.service.sync_manager.SyncManager.sync") as mock_sync:
        mock_sync.return_value = None
        
        # 第一次请求
        response1 = client.post("/api/v1/sync", json={
            "table_ids": [table_id],
            "formats": ["csv"]
        })
        
        assert response1.status_code == 200
        assert table_id not in ACTIVE_SYNC_TASKS, "同步完成后锁应该被释放"

def test_sync_task_locking_multiple_tables():
    """
    测试多表同步的锁管理
    """
    table_ids = ["test.lock.multi1", "test.lock.multi2"]
    ACTIVE_SYNC_TASKS.clear()
    
    with patch("app.service.sync_manager.SyncManager.sync") as mock_sync:
        mock_sync.return_value = None
        
        response = client.post("/api/v1/sync", json={
            "table_ids": table_ids,
            "formats": ["csv"]
        })
        
        assert response.status_code == 200
        # 两个表都应该被锁定然后释放
        assert "test.lock.multi1" not in ACTIVE_SYNC_TASKS
        assert "test.lock.multi2" not in ACTIVE_SYNC_TASKS

def test_sync_task_locking_concurrent_different_tables():
    """
    测试不同表的并发同步：应该可以同时进行
    """
    table_id1 = "test.lock.concurrent1"
    table_id2 = "test.lock.concurrent2"
    ACTIVE_SYNC_TASKS.clear()
    
    with patch("app.service.sync_manager.SyncManager.sync") as mock_sync:
        mock_sync.return_value = None
        
        # 第一个表同步
        response1 = client.post("/api/v1/sync", json={
            "table_ids": [table_id1],
            "formats": ["csv"]
        })
        
        # 第二个表同步（应该可以同时进行）
        response2 = client.post("/api/v1/sync", json={
            "table_ids": [table_id2],
            "formats": ["csv"]
        })
        
        assert response1.status_code == 200
        assert response2.status_code == 200

def test_sync_task_locking_exception_release():
    """
    测试异常时锁也能正确释放
    """
    table_id = "test.lock.exception"
    ACTIVE_SYNC_TASKS.clear()
    
    with patch("app.service.sync_manager.SyncManager.sync") as mock_sync:
        mock_sync.side_effect = RuntimeError("模拟同步异常")
        
        response = client.post("/api/v1/sync", json={
            "table_ids": [table_id],
            "formats": ["csv"]
        })
        
        # 即使异常，锁也应该被释放
        assert table_id not in ACTIVE_SYNC_TASKS

def test_sync_task_locking_validation():
    """
    测试请求参数验证
    """
    # 缺少 table_ids - 应该返回 422
    response1 = client.post("/api/v1/sync", json={
        "formats": ["csv"]
    })
    assert response1.status_code == 422
    
    # 缺少 formats - formats 有默认值 ["csv]，所以应该返回 200
    response2 = client.post("/api/v1/sync", json={
        "table_ids": ["test.table"]
    })
    assert response2.status_code == 200
    
    # 空 table_ids - 应该返回 200（空列表是有效的）
    response3 = client.post("/api/v1/sync", json={
        "table_ids": [],
        "formats": ["csv"]
    })
    assert response3.status_code == 200

def test_sync_task_locking_force_refresh():
    """
    测试强制刷新参数传递
    """
    table_id = "test.lock.force"
    ACTIVE_SYNC_TASKS.clear()
    
    with patch("app.service.sync_manager.SyncManager.sync") as mock_sync:
        mock_sync.return_value = None
        
        response = client.post("/api/v1/sync", json={
            "table_ids": [table_id],
            "formats": ["csv"],
            "force_refresh": True
        })
        
        assert response.status_code == 200
        # 验证 force_refresh 参数被传递
        mock_sync.assert_called_once()
        call_args = mock_sync.call_args
        assert call_args.kwargs.get("force_refresh") == True