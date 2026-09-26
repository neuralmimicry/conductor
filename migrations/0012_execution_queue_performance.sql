CREATE INDEX IF NOT EXISTS work_executions_active_updated_at_idx
    ON work_executions (updated_at ASC)
    WHERE status IN ('pending', 'planning', 'submitted', 'running', 'verifying');

CREATE INDEX IF NOT EXISTS work_items_execution_queue_age_idx
    ON work_items (COALESCE(scheduled_for, created_at) ASC, priority DESC)
    WHERE execution_approved = TRUE AND status = 'scheduled';
