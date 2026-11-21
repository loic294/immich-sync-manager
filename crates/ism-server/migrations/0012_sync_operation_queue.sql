-- Pending peer operations participate in the same change stream as file transfers.
-- Keeping the change identifier in the existing table avoids a second peer cursor.
CREATE TRIGGER sync_pending_operation_insert AFTER INSERT ON pending_peer_ops BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    VALUES ('operation:' || NEW.operation_id || ':' || NEW.idx, NEW.peer_id);
END;
CREATE TRIGGER sync_pending_operation_delete AFTER DELETE ON pending_peer_ops BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    VALUES ('operation:' || OLD.operation_id || ':' || OLD.idx, OLD.peer_id);
END;
CREATE TRIGGER sync_pending_event_insert AFTER INSERT ON pending_peer_events BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    VALUES ('operation:' || NEW.operation_id || ':0', NEW.peer_id);
END;
CREATE TRIGGER sync_pending_event_delete AFTER DELETE ON pending_peer_events BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    VALUES ('operation:' || OLD.operation_id || ':0', OLD.peer_id);
END;
CREATE TRIGGER sync_pending_operation_update AFTER UPDATE OF status,finished_at ON operations BEGIN
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    SELECT 'operation:' || operation_id || ':' || idx, peer_id
    FROM pending_peer_ops WHERE operation_id = NEW.id;
    INSERT OR REPLACE INTO sync_changes (transfer_id, peer_id)
    SELECT 'operation:' || operation_id || ':0', peer_id
    FROM pending_peer_events WHERE operation_id = NEW.id;
END;
