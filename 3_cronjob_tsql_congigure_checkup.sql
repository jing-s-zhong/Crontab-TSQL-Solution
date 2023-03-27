USE [_METADATA]
GO

SELECT [crn_id]
      ,[crn_name]
      ,[crn_cronjob]
      ,[crn_enabled]
FROM [dbo].[cronjob_schedule];

SELECT [tsk_id]
      ,[tsk_name]
      ,[tsk_enabled]
      ,[tsk_skippable]
      ,[tsk_crn_id]
FROM [dbo].[cronjob_task];

SELECT TOP (1000) [stp_id]
      ,[stp_tsk_id]
      ,[stp_step_id]
      ,[stp_name]
      ,[stp_type]
	  ,[stp_failure_continue]
      ,[stp_script]
FROM [_METADATA].[dbo].[cronjob_step]
ORDER BY [stp_tsk_id]
	,[stp_step_id];

SELECT TOP (1000) [run_id]
      ,[run_tsk_id]
      ,[run_crn_id]
      ,[run_status]
      ,[run_this_run]
      ,[run_next_run]
      ,[run_runner_id]
FROM [dbo].[cronjob_running];

SELECT TOP (1000) [his_id]
      ,[his_tsk_id]
      ,[his_crn_id]
      ,[his_status]
      ,[his_scheduled]
      ,[his_executed]
      ,[his_completed]
      ,[his_runner_id]
	  ,[his_detail]
FROM [dbo].[cronjob_history]
ORDER BY [his_id] DESC;
GO

