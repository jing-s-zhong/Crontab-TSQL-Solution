USE [_METADATA]
GO

TRUNCATE TABLE [dbo].[cronjob_history];

DELETE FROM [dbo].[cronjob_step]
WHERE [stp_name] LIKE 'example_task[0-9]_step[0-9]';


DELETE FROM [dbo].[cronjob_task]
WHERE [tsk_name] LIKE 'example_task_[0-9]';


DELETE FROM [dbo].[cronjob_schedule]
WHERE [crn_name] LIKE 'example_schedule_[0-9]';


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cronjob_demo]') AND type in (N'U'))
DROP TABLE [dbo].[cronjob_demo];
GO


