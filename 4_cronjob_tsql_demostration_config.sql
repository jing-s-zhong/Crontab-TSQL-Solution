USE [_METADATA]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cronjob_demo]') AND type in (N'U'))
DROP TABLE [dbo].[cronjob_demo];
GO
CREATE TABLE [dbo].[cronjob_demo](
	[demo_id] [int] IDENTITY(1,1) NOT NULL,
	[demo_ts] [datetime] NOT NULL,
	[demo_msg] [nvarchar](max) NULL
);
GO

INSERT [dbo].[cronjob_schedule] ([crn_id], [crn_name], [crn_cronjob], [crn_enabled]) VALUES (1, N'example_schedule_1', N'0 4 * * *', 1)
GO
INSERT [dbo].[cronjob_schedule] ([crn_id], [crn_name], [crn_cronjob], [crn_enabled]) VALUES (2, N'example_schedule_2', N'0 7 * * *', 1)
GO
INSERT [dbo].[cronjob_schedule] ([crn_id], [crn_name], [crn_cronjob], [crn_enabled]) VALUES (3, N'example_schedule_3', N'0/15 5-21 * * *', 1)
GO
INSERT [dbo].[cronjob_schedule] ([crn_id], [crn_name], [crn_cronjob], [crn_enabled]) VALUES (4, N'example_schedule_4', N'5/15 6-22 * * *', 1)
GO

SET IDENTITY_INSERT [dbo].[cronjob_task] ON 
GO
INSERT [dbo].[cronjob_task] ([tsk_id], [tsk_name], [tsk_enabled], [tsk_skippable], [tsk_crn_id]) VALUES (1, N'example_task_1', 1, 1, 1)
GO
INSERT [dbo].[cronjob_task] ([tsk_id], [tsk_name], [tsk_enabled], [tsk_skippable], [tsk_crn_id]) VALUES (2, N'example_task_2', 1, 0, 2)
GO
INSERT [dbo].[cronjob_task] ([tsk_id], [tsk_name], [tsk_enabled], [tsk_skippable], [tsk_crn_id]) VALUES (3, N'example_task_3', 1, 1, 3)
GO
INSERT [dbo].[cronjob_task] ([tsk_id], [tsk_name], [tsk_enabled], [tsk_skippable], [tsk_crn_id]) VALUES (4, N'example_task_4', 1, 0, 4)
GO
INSERT [dbo].[cronjob_task] ([tsk_id], [tsk_name], [tsk_enabled], [tsk_skippable], [tsk_crn_id]) VALUES (5, N'example_task_5', 1, 1, 3)
GO
SET IDENTITY_INSERT [dbo].[cronjob_task] OFF
GO

INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (1, 1, 1, N'example_task1_step1', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task1_step1'');')
GO
INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (2, 2, 1, N'example_task2_step1', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task2_step1'');')
GO
INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (3, 3, 1, N'example_task3_step1', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task3_step1'');')
GO
INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (4, 4, 1, N'example_task4_step1', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task4_step1'');')
GO
INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (5, 5, 1, N'example_task5_step1', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task5_step1'');')
GO
INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (6, 1, 2, N'example_task1_step2', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task1_step2'');')
GO
INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (7, 2, 2, N'example_task2_step2', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task2_step2'');')
GO
INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (8, 3, 2, N'example_task3_step2', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task3_step2'');')
GO
INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (9, 2, 3, N'example_task2_step3', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task2_step3'');')
GO
INSERT [dbo].[cronjob_step] ([stp_id], [stp_tsk_id], [stp_step_id], [stp_name], [stp_type], [stp_script]) VALUES (10, 3, 3, N'example_task3_step3', 1, N'INSERT INTO [dbo].[cronjob_demo]([demo_ts], [demo_msg]) VALUES (GETDATE(), ''example_task3_step3'');')
GO
