USE [_METADATA]
GO

DROP PROCEDURE [dbo].[cronjob_task_resolver]
GO

DROP PROCEDURE [dbo].[cronjob_task_runner]
GO

DROP PROCEDURE [dbo].[cronjob_task_picker]
GO

DROP PROCEDURE [dbo].[cronjob_task_scheduler]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cronjob_task]') AND type in (N'U'))
DROP TABLE [dbo].[cronjob_task]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cronjob_schedule]') AND type in (N'U'))
DROP TABLE [dbo].[cronjob_schedule]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cronjob_running]') AND type in (N'U'))
DROP TABLE [dbo].[cronjob_running]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cronjob_history]') AND type in (N'U'))
DROP TABLE [dbo].[cronjob_history]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cronjob_step]') AND type in (N'U'))
DROP TABLE [dbo].[cronjob_step]
GO

DROP FUNCTION [dbo].[crontab_decode]
GO

DROP FUNCTION [dbo].[cronjob_trigger]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Jing S. Zhong
-- Create date: 2023-02-16
-- Description:	This is a T-SQL function to parse the crontab
-- =============================================
/*
	SELECT dbo.cronjob_trigger ('17 12-20 17-23,29 2 0,6',NULL,'Monthly');
*/
CREATE FUNCTION [dbo].[cronjob_trigger] (
	@cron [nvarchar](MAX),
	@starttime [datetime2](0) = NULL,
	@frequency [nvarchar](10) = 'Daily' --  Hourly, Daily, Weekly, Monthly; Default Daily
	)
RETURNS [datetime]
AS
BEGIN
	DECLARE @RESULT [datetime];
	--DECLARE @cron [nvarchar](100) = '32 18 17,21-23,29 * 1-5'
	--DECLARE @starttime [datetime] = NULL --'2023-02-15 11:22:33';
	--DECLARE @frequency [nvarchar](100) = 'Monthly';-- Hourly, Daily, Weekly, Monthly; Default Daily

	/**
	 ** Compute Time Scope
	 **/
	DECLARE @datetime1 [datetime] = DATEADD(mi, DATEDIFF(mi, 0, ISNULL(@starttime, GETDATE())), 0);
	DECLARE @datetime2 [datetime] = CASE 
		WHEN @frequency IN ('Hourly') THEN DATEADD(mi, - 1, dateadd(hh, 1, @datetime1))
		WHEN @frequency IN ('Daily') THEN DATEADD(mi, - 1, dateadd(dd, 1, @datetime1))
		WHEN @frequency IN ('Weekly') THEN DATEADD(mi, - 1, dateadd(wk, 1, @datetime1))
		WHEN @frequency IN ('Monthly') THEN DATEADD(mi, - 1, dateadd(mm, 1, @datetime1))
		ELSE DATEADD(mi, - 1, dateadd(dd, 1, @datetime1))
		END;

	/**
	 ** Define Work Variables
	 **/
	DECLARE @SubItems TABLE ([key] [int] IDENTITY(0, 1), [value] [nvarchar](MAX));
	DECLARE @key [nvarchar](MAX), @value [nvarchar](MAX);
	DECLARE @CronItems TABLE ([key] [int] IDENTITY(0, 1), [value] [nvarchar](MAX));
	DECLARE @subKey [nvarchar](MAX), @subValue [nvarchar](MAX);
	DECLARE @start INT, @stop INT, @step INT;
	DECLARE @Iterations TABLE ([ymd] [date], [key] [nvarchar](4000), [iteration] INT);

	/**
	 ** Docode crontab
	 **/
	DECLARE @CronItemsXml [xml] = CONVERT([xml], REPLACE(REPLACE(REPLACE('[' + REPLACE(@cron, ' ', '][') + ']', '[]', ''), '[', '<item>'), ']', '</item>'));
	INSERT INTO @CronItems ([value])
	SELECT cron.item.value('.', '[nvarchar](MAX)') [value]
	FROM @CronItemsXml.nodes('item') cron(item);


	/**
	 ** Scan cron Items
	 **/
	DECLARE CronItems CURSOR FOR
	SELECT [key], [value]
	FROM @CronItems;

	OPEN CronItems;
	FETCH CronItems INTO @key, @value;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		/*
		PRINT REPLICATE(CHAR(09), 1) + 'CronItem[' + @key + '](' + CASE @key
			WHEN 0 THEN 'Minute-Of-Hour'
			WHEN 1 THEN 'Hour-Of-Day'
			WHEN 2 THEN 'Day-Of-Month'
			WHEN 3 THEN 'Month-Of-Year'
			WHEN 4 THEN 'Day-Of-Week'
			END + '): ' + @value;
		*/

		/**
		 ** Docode SubList
		 **/
		DELETE FROM @SubItems;
		DECLARE @subItemsXml [xml] = CONVERT([xml], '<subitem>' + REPLACE(@value, ',', '</subitem><subitem>') + '</subitem>');
		INSERT INTO @SubItems ([value])
		SELECT item.subitem.value('.', '[nvarchar](MAX)') [value]
		FROM @subItemsXml.nodes('subitem') item(subitem);

		/**
		 ** Scan Sub Items
		 **/
		DECLARE SubList CURSOR FOR
		SELECT *
		FROM @SubItems;

		OPEN SubList;
		FETCH SubList INTO @subKey, @subValue;
		WHILE @@FETCH_STATUS = 0
		BEGIN
			/**
			 ** Iterate Sub Items
			 **/
			DECLARE @part1 [nvarchar](10) = SUBSTRING(@subValue, 1, ISNULL(NULLIF(CHARINDEX('-', @subValue), 0), LEN(@subValue) + 1) - 1);
			DECLARE @part2 [nvarchar](10) = SUBSTRING(@subValue, CHARINDEX('-', @subValue) + 1, ISNULL(NULLIF(CHARINDEX('/', @subValue, CHARINDEX('-', @subValue)), 0) -1, LEN(@subValue)));
			DECLARE @part3 [nvarchar](10) = ISNULL(NULLIF(SUBSTRING(@part1, ISNULL(NULLIF(CHARINDEX('/', @part1), 0), LEN(@part1)) + 1, LEN(@part1)), ''), '1');

			SET @part1 = SUBSTRING(@part1, 1, ISNULL(NULLIF(CHARINDEX('/', @part1), 0), LEN(@part1) + 1) - 1);
			SET @part2 = CASE WHEN @part3 != '1' THEN ISNULL(NULLIF(@part2, @part1), '*') ELSE @part2 END;

			IF ISNUMERIC(@part1) = 1
				SET @start = CONVERT(INT, @part1)
			ELSE IF @part1 = '*'
				SET @start = CASE @key
					WHEN 0 THEN 0
					WHEN 1 THEN 0
					WHEN 2 THEN 1
					WHEN 3 THEN 1
					WHEN 4 THEN 0
					ELSE 7
					END;

			IF ISNUMERIC(@part2) = 1
				SET @stop = CONVERT(INT, @part2)
			ELSE IF @part2 = '*'
				SET @stop = CASE @key
					WHEN 0 THEN 59
					WHEN 1 THEN 23
					WHEN 2 THEN 31
					WHEN 3 THEN 12
					WHEN 4 THEN 6
					ELSE 7
					END;

			IF ISNUMERIC(@part3) = 1
				SET @step = CONVERT(INT, @part3)
			ELSE SET @step = 1;

			--PRINT REPLICATE(CHAR(09), 2) + 'SubItem[' + @subKey + ']: ' + @subValue + ' => ' + @part1 + ' : ' + @part2 + ' : ' + @part3;

			WHILE @start <= @stop
			BEGIN
				--PRINT REPLICATE(CHAR(09), 3) + 'Ierate: ' + CONVERT([nvarchar], @start);

				INSERT INTO @Iterations ([ymd], [key], [iteration])
				VALUES (NULL, @key, @start);

				SET @start = @start + @step;
			END

			FETCH SubList INTO @subKey, @subValue;
		END

		CLOSE SubList;
		DEALLOCATE SubList;

		FETCH CronItems INTO @key, @value;
	END

	CLOSE CronItems;
	DEALLOCATE CronItems;

	/**
	 ** Iterate Scoped Dates
	 **/
	DECLARE @YearMonthDay [date], @YearMonthDayInt INT;
	SET @YearMonthDay = DATEADD(M, DATEDIFF(M, 0, @datetime1), 0);
	WHILE @YearMonthDay <= @datetime2
	BEGIN
		IF EXISTS (
			SELECT DISTINCT [iteration]
			FROM @Iterations
			WHERE [iteration] = DATEPART(weekday, @YearMonthDay)
				AND [key] = 4
			)
		AND EXISTS (
			SELECT DISTINCT [iteration]
			FROM @Iterations
			WHERE [iteration] = DATEPART(month, @YearMonthDay)
				AND [key] = 3
			)
		AND EXISTS (
			SELECT DISTINCT [iteration]
			FROM @Iterations
			WHERE [iteration] = DATEPART(day, @YearMonthDay)
				AND [key] = 2
			)
		BEGIN
			--PRINT 'Year-Month-Day: ' + FORMAT(@YearMonthDayInt, '0000-00-00');
			SET @YearMonthDayInt = 10000 * YEAR(@YearMonthDay) + 100 * MONTH(@YearMonthDay) + DAY(@YearMonthDay);
			INSERT INTO @Iterations ([ymd], [key], [iteration])
			VALUES (NULL, '9', @YearMonthDayInt);
		END

		SET @YearMonthDay = DATEADD(D, 1, @YearMonthDay);
	END;

	/**
	 ** Join the Date-Hour-Minute
	 **/
	WITH [Trig_List] AS (
		SELECT DISTINCT T.*, DATEPART(weekday, T.[trigger_time]) dow
			,ROW_NUMBER() OVER(ORDER BY [trigger_time]) [trig_sequence]
		FROM (
			SELECT DATETIMEFROMPARTS(
					T9.[iteration] / 10000, (T9.[iteration] % 10000) / 100
					,T9.[iteration] % 100
					,T1.[iteration]
					,T0.[iteration]
					,0, 0 ) [trigger_time]
				,FORMAT(T9.[iteration], '00000000') [yyyymmdd]
				,FORMAT(T9.[iteration] / 10000, '0000') [yyyy]
				,FORMAT(T0.[iteration], '00') [min]
				,FORMAT(T1.[iteration], '00') [hh]
				,FORMAT(T9.[iteration] % 100, '00') [dd]
				,FORMAT((T9.[iteration] % 10000) / 100, '00') [mm]
			FROM ( SELECT * FROM @Iterations WHERE [key] = 9 ) T9,
				( SELECT * FROM @Iterations WHERE [key] = 1 ) T1,
				( SELECT * FROM @Iterations WHERE [key] = 0 ) T0
			) T
		WHERE [trigger_time] >= @datetime1 --AND @datetime2
		)
	SELECT @RESULT = B.[trigger_time]
	FROM [Trig_List] A
	JOIN [Trig_List] B
		ON A.[trig_sequence] = 1
		AND B.[trig_sequence] = 2
	WHERE A.[trigger_time] = @datetime1

	RETURN @RESULT;
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Jing S. Zhong
-- Create date: 2023-02-16
-- Description:	This is a T-SQL function to parse the crontab
-- =============================================
/*
	SELECT * FROM dbo.crontab_decode ('32 18 17,21-23,29 2 1-5',NULL,'Monthly');
*/
CREATE FUNCTION [dbo].[crontab_decode] (
	@cron [nvarchar](MAX),
	@starttime [datetime2](0) = NULL,
	@frequency [nvarchar](10) = 'Daily' --  Hourly, Daily, Weekly, Monthly; Default Daily
	)
RETURNS @RESULT TABLE
(
	[trigger_time] [datetime] NULL,
	[yyyymmdd] [nvarchar](8) NULL,
	[yyyy] [nvarchar](4) NULL,
	[min] [nvarchar](2) NULL,
	[hh] [nvarchar](8) NULL,
	[dd] [nvarchar](2) NULL,
	[mm] [nvarchar](10) NULL,
	[dow] [int] NULL
)
AS
BEGIN
	--DECLARE @cron [nvarchar](100) = '32 18 17,21-23,29 * 1-5'
	--DECLARE @starttime [datetime] = NULL --'2023-02-15 11:22:33';
	--DECLARE @frequency [nvarchar](100) = 'Monthly';-- Hourly, Daily, Weekly, Monthly; Default Daily

	/**
	 ** Compute Time Scope
	 **/
	DECLARE @datetime1 [datetime] = DATEADD(mi, DATEDIFF(mi, 0, ISNULL(@starttime, GETDATE())), 0);
	DECLARE @datetime2 [datetime] = CASE 
		WHEN @frequency IN ('Hourly') THEN DATEADD(mi, - 1, dateadd(hh, 1, @datetime1))
		WHEN @frequency IN ('Daily') THEN DATEADD(mi, - 1, dateadd(dd, 1, @datetime1))
		WHEN @frequency IN ('Weekly') THEN DATEADD(mi, - 1, dateadd(wk, 1, @datetime1))
		WHEN @frequency IN ('Monthly') THEN DATEADD(mi, - 1, dateadd(mm, 1, @datetime1))
		ELSE DATEADD(mi, - 1, dateadd(dd, 1, @datetime1))
		END;

	/**
	 ** Define Work Variables
	 **/
	DECLARE @SubItems TABLE ([key] [int] IDENTITY(0, 1), [value] [nvarchar](MAX));
	DECLARE @key [nvarchar](MAX), @value [nvarchar](MAX);
	DECLARE @CronItems TABLE ([key] [int] IDENTITY(0, 1), [value] [nvarchar](MAX));
	DECLARE @subKey [nvarchar](MAX), @subValue [nvarchar](MAX);
	DECLARE @start INT, @stop INT, @step INT;
	DECLARE @Iterations TABLE ([ymd] [date], [key] [nvarchar](4000), [iteration] INT);

	/**
	 ** Docode crontab
	 **/
	DECLARE @CronItemsXml [xml] = CONVERT([xml], REPLACE(REPLACE(REPLACE('[' + REPLACE(@cron, ' ', '][') + ']', '[]', ''), '[', '<item>'), ']', '</item>'));
	INSERT INTO @CronItems ([value])
	SELECT cron.item.value('.', '[nvarchar](MAX)') [value]
	FROM @CronItemsXml.nodes('item') cron(item);


	/**
	 ** Scan cron Items
	 **/
	DECLARE CronItems CURSOR FOR
	SELECT [key], [value]
	FROM @CronItems;

	OPEN CronItems;
	FETCH CronItems INTO @key, @value;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		/*
		PRINT REPLICATE(CHAR(09), 1) + 'CronItem[' + @key + '](' + CASE @key
			WHEN 0 THEN 'Minute-Of-Hour'
			WHEN 1 THEN 'Hour-Of-Day'
			WHEN 2 THEN 'Day-Of-Month'
			WHEN 3 THEN 'Month-Of-Year'
			WHEN 4 THEN 'Day-Of-Week'
			END + '): ' + @value;
		*/

		/**
		 ** Docode SubList
		 **/
		DELETE FROM @SubItems;
		DECLARE @subItemsXml [xml] = CONVERT([xml], '<subitem>' + REPLACE(@value, ',', '</subitem><subitem>') + '</subitem>');
		INSERT INTO @SubItems ([value])
		SELECT item.subitem.value('.', '[nvarchar](MAX)') [value]
		FROM @subItemsXml.nodes('subitem') item(subitem);

		/**
		 ** Scan Sub Items
		 **/
		DECLARE SubList CURSOR FOR
		SELECT *
		FROM @SubItems;

		OPEN SubList;
		FETCH SubList INTO @subKey, @subValue;
		WHILE @@FETCH_STATUS = 0
		BEGIN
			/**
			 ** Iterate Sub Items
			 **/
			DECLARE @part1 [nvarchar](10) = SUBSTRING(@subValue, 1, ISNULL(NULLIF(CHARINDEX('-', @subValue), 0), LEN(@subValue) + 1) - 1);
			DECLARE @part2 [nvarchar](10) = SUBSTRING(@subValue, CHARINDEX('-', @subValue) + 1, ISNULL(NULLIF(CHARINDEX('/', @subValue, CHARINDEX('-', @subValue)), 0) -1, LEN(@subValue)));
			DECLARE @part3 [nvarchar](10) = ISNULL(NULLIF(SUBSTRING(@part1, ISNULL(NULLIF(CHARINDEX('/', @part1), 0), LEN(@part1)) + 1, LEN(@part1)), ''), '1');

			SET @part1 = SUBSTRING(@part1, 1, ISNULL(NULLIF(CHARINDEX('/', @part1), 0), LEN(@part1) + 1) - 1);
			SET @part2 = CASE WHEN @part3 != '1' THEN ISNULL(NULLIF(@part2, @part1), '*') ELSE @part2 END;

			IF ISNUMERIC(@part1) = 1
				SET @start = CONVERT(INT, @part1)
			ELSE IF @part1 = '*'
				SET @start = CASE @key
					WHEN 0 THEN 0
					WHEN 1 THEN 0
					WHEN 2 THEN 1
					WHEN 3 THEN 1
					WHEN 4 THEN 0
					ELSE 7
					END;

			IF ISNUMERIC(@part2) = 1
				SET @stop = CONVERT(INT, @part2)
			ELSE IF @part2 = '*'
				SET @stop = CASE @key
					WHEN 0 THEN 59
					WHEN 1 THEN 23
					WHEN 2 THEN 31
					WHEN 3 THEN 12
					WHEN 4 THEN 6
					ELSE 7
					END;

			IF ISNUMERIC(@part3) = 1
				SET @step = CONVERT(INT, @part3)
			ELSE SET @step = 1;

			--PRINT REPLICATE(CHAR(09), 2) + 'SubItem[' + @subKey + ']: ' + @subValue + ' => ' + @part1 + ' : ' + @part2 + ' : ' + @part3;

			WHILE @start <= @stop
			BEGIN
				--PRINT REPLICATE(CHAR(09), 3) + 'Ierate: ' + CONVERT([nvarchar], @start);

				INSERT INTO @Iterations ([ymd], [key], [iteration])
				VALUES (NULL, @key, @start);

				SET @start = @start + @step;
			END

			FETCH SubList INTO @subKey, @subValue;
		END

		CLOSE SubList;
		DEALLOCATE SubList;

		FETCH CronItems INTO @key, @value;
	END

	CLOSE CronItems;
	DEALLOCATE CronItems;

	/**
	 ** Iterate Scoped Dates
	 **/
	DECLARE @YearMonthDay [date], @YearMonthDayInt INT;
	SET @YearMonthDay = DATEADD(M, DATEDIFF(M, 0, @datetime1), 0);
	WHILE @YearMonthDay <= @datetime2
	BEGIN
		IF EXISTS (
			SELECT DISTINCT [iteration]
			FROM @Iterations
			WHERE [iteration] = DATEPART(weekday, @YearMonthDay)
				AND [key] = 4
			)
		AND EXISTS (
			SELECT DISTINCT [iteration]
			FROM @Iterations
			WHERE [iteration] = DATEPART(month, @YearMonthDay)
				AND [key] = 3
			)
		AND EXISTS (
			SELECT DISTINCT [iteration]
			FROM @Iterations
			WHERE [iteration] = DATEPART(day, @YearMonthDay)
				AND [key] = 2
			)
		BEGIN
			--PRINT 'Year-Month-Day: ' + FORMAT(@YearMonthDayInt, '0000-00-00');
			SET @YearMonthDayInt = 10000 * YEAR(@YearMonthDay) + 100 * MONTH(@YearMonthDay) + DAY(@YearMonthDay);
			INSERT INTO @Iterations ([ymd], [key], [iteration])
			VALUES (NULL, '9', @YearMonthDayInt);
		END

		SET @YearMonthDay = DATEADD(D, 1, @YearMonthDay);
	END;

	/**
	 ** Join the Date-Hour-Minute
	 **/
	WITH Trig_List AS (
		SELECT DISTINCT T.*, DATEPART(weekday, T.[trigger_time]) dow
		FROM (
			SELECT DATETIMEFROMPARTS(
					T9.[iteration] / 10000, (T9.[iteration] % 10000) / 100
					,T9.[iteration] % 100
					,T1.[iteration]
					,T0.[iteration]
					,0, 0 ) [trigger_time]
				,FORMAT(T9.[iteration], '00000000') [yyyymmdd]
				,FORMAT(T9.[iteration] / 10000, '0000') [yyyy]
				,FORMAT(T0.[iteration], '00') [min]
				,FORMAT(T1.[iteration], '00') [hh]
				,FORMAT(T9.[iteration] % 100, '00') [dd]
				,FORMAT((T9.[iteration] % 10000) / 100, '00') [mm]
			FROM ( SELECT * FROM @Iterations WHERE [key] = 9 ) T9,
				( SELECT * FROM @Iterations WHERE [key] = 1 ) T1,
				( SELECT * FROM @Iterations WHERE [key] = 0 ) T0
			) T
		WHERE [trigger_time] >= @datetime1 --AND @datetime2
		)
	INSERT INTO @RESULT
	SELECT DISTINCT *
	FROM Trig_List T
	WHERE [trigger_time] BETWEEN @datetime1 AND @datetime2
	ORDER BY [trigger_time];

	RETURN;
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[cronjob_step](
	[stp_id] [int] NOT NULL,
	[stp_tsk_id] [int] NOT NULL,
	[stp_step_id] [int] NOT NULL,
	[stp_name] [nvarchar](250) NULL,
	[stp_type] [bit] NULL,
	[stp_failure_continue] [bit] NULL,
	[stp_script] [nvarchar](max) NULL,
 CONSTRAINT [PK_cronjob_step] PRIMARY KEY CLUSTERED 
(
	[stp_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[cronjob_history](
	[his_id] [int] IDENTITY(1,1) NOT NULL,
	[his_tsk_id] [int] NOT NULL,
	[his_crn_id] [int] NOT NULL,
	[his_status] [nvarchar](50) NOT NULL,
	[his_scheduled] [datetime] NOT NULL,
	[his_executed] [datetime] NULL,
	[his_completed] [datetime] NULL,
	[his_runner_id] [int] NULL,
	[his_detail] [nvarchar](max) NULL,
 CONSTRAINT [PK_cronjob_history] PRIMARY KEY CLUSTERED 
(
	[his_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[cronjob_running](
	[run_id] [int] IDENTITY(1,1) NOT NULL,
	[run_tsk_id] [int] NOT NULL,
	[run_crn_id] [int] NOT NULL,
	[run_status] [nvarchar](50) NOT NULL,
	[run_this_run] [datetime] NOT NULL,
	[run_next_run] [datetime] NULL,
	[run_runner_id] [int] NULL,
 CONSTRAINT [PK_cronjob_running] PRIMARY KEY CLUSTERED 
(
	[run_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[cronjob_schedule](
	[crn_id] [int] NOT NULL,
	[crn_name] [nvarchar](128) NOT NULL,
	[crn_cronjob] [nvarchar](50) NULL,
	[crn_enabled] [bit] NULL,
 CONSTRAINT [PK_schedule_crontab] PRIMARY KEY CLUSTERED 
(
	[crn_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[cronjob_task](
	[tsk_id] [int] IDENTITY(1,1) NOT NULL,
	[tsk_name] [nvarchar](250) NULL,
	[tsk_enabled] [bit] NULL,
	[tsk_skippable] [int] NULL,
	[tsk_crn_id] [int] NULL,
 CONSTRAINT [PK_cronjob_task] PRIMARY KEY CLUSTERED 
(
	[tsk_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Jing S. Zhong
-- Create date: 2023-02-19
-- Description:	cronjob_task_scheduler
-- =============================================
CREATE PROCEDURE [dbo].[cronjob_task_scheduler]
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @ARCHIEVE TABLE (
		[action_type] [nvarchar](50),
		[run_id] [int] NOT NULL,
		[run_tsk_id] [int] NOT NULL,
		[run_crn_id] [int] NOT NULL,
		[run_status] [nvarchar](50) NOT NULL,
		[run_this_run] [datetime] NOT NULL,
		[run_next_run] [datetime] NULL,
		[run_runner_id] [int]
	);

	WITH [task_triggered] AS (
		SELECT tsk.[tsk_id] [run_tsk_id]
			,crn.[crn_id] [run_crn_id]
			,'pending' [run_status]
			,getdate() [run_this_run]
			,[dbo].[cronjob_trigger]([crn_cronjob],null,null) [run_next_run]
			,ISNULL(tsk.[tsk_skippable], 1) [tsk_skippable]
		FROM [cronjob_task] tsk
		JOIN [cronjob_schedule] crn
		ON tsk.[tsk_crn_id] = crn.[crn_id]
		WHERE [crn_enabled] = 1
			AND [dbo].[cronjob_trigger]([crn_cronjob],null,null) IS NOT NULL
		)
	MERGE INTO [dbo].[cronjob_running] D
	USING [task_triggered] S
	ON D.[run_tsk_id] = S.[run_tsk_id]
	AND D.[run_crn_id] = S.[run_crn_id]
	AND DATEDIFF(N, '1900-01-01', D.[run_this_run]) = DATEDIFF(N, '1900-01-01', S.[run_this_run])
	WHEN MATCHED AND S.[run_next_run] IS NOT NULL
		THEN UPDATE SET [run_next_run] = S.[run_next_run]
	WHEN NOT MATCHED BY TARGET AND (
			S.[tsk_skippable] = 0
			OR NOT EXISTS (
				SELECT *
				FROM [dbo].[cronjob_running] D
				JOIN [task_triggered] S
				ON D.[run_tsk_id] = S.[run_tsk_id]
				AND D.[run_crn_id] = S.[run_crn_id]
				WHERE D.[run_status] NOT IN ('pending', 'running')
				)
			)
		THEN INSERT ([run_tsk_id]
			   ,[run_crn_id]
			   ,[run_status]
			   ,[run_this_run]
			   ,[run_next_run]
			   )
			VALUES (S.[run_tsk_id]
			   ,S.[run_crn_id]
			   ,S.[run_status]
			   ,S.[run_this_run]
			   ,S.[run_next_run]
			   )
	OUTPUT $action AS [action_type]
		,inserted.*
	INTO @ARCHIEVE;

	--SELECT * FROM @ARCHIEVE;

	INSERT INTO [dbo].[cronjob_history] (
		[his_tsk_id]
        ,[his_crn_id]
        ,[his_status]
        ,[his_scheduled]
        --,[his_executed]
        --,[his_completed]
		)
	SELECT [run_tsk_id]
		  ,[run_crn_id]
		  ,[run_status]
		  ,[run_this_run]
		  --,NULL
	FROM @ARCHIEVE
	WHERE [action_type] = 'INSERT';
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Jing S. Zhong
-- Create date: 2023-02-19
-- Description:	cronjob_task_picker
-- =============================================
/*
EXEC [dbo].[cronjob_task_picker]
	@RUNNER_ID = 1,
	@STATUS = 'running'
*/
CREATE PROCEDURE [dbo].[cronjob_task_picker]
	@RUNNER_ID INT = 1,
	@STATUS [nvarchar](50) = 'running'
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @ARCHIEVE TABLE (
		[run_id] [int] NOT NULL,
		[run_tsk_id] [int] NOT NULL,
		[run_crn_id] [int] NOT NULL,
		[run_status] [nvarchar](50) NOT NULL,
		[run_this_run] [datetime] NOT NULL,
		[run_next_run] [datetime] NULL,
		[run_runner_id] [int] NULL
	);

	BEGIN TRAN
	BEGIN TRY
		WITH [task_picked] AS (
			SELECT TOP (1) [run_id]
					,[run_tsk_id]
					,[run_crn_id]
					,[run_status]
					,[run_this_run]
					,[run_next_run]
					,[run_runner_id]
			FROM [dbo].[cronjob_running]
			WHERE [run_status] = 'pending'
			ORDER BY [run_id]
			)
		UPDATE [task_picked]
		SET [run_runner_id] = @RUNNER_ID 
			,[run_status] = @STATUS
		OUTPUT inserted.*
		INTO @ARCHIEVE;

		UPDATE his
		SET [his_runner_id] = [run_runner_id]
			,[his_status] = [run_status]
			,[his_executed] = GETDATE()
		FROM [dbo].[cronjob_history] his
		JOIN @ARCHIEVE que
		ON his.[his_tsk_id] = que.[run_tsk_id]
		AND his.[his_crn_id] = que.[run_crn_id]
		AND his.[his_scheduled] = que.[run_this_run]
		COMMIT;

		SELECT * FROM @ARCHIEVE;

	END TRY
	BEGIN CATCH
		-- handle the failure with que and his
		ROLLBACK;
		PRINT '--- rollback ---'
		PRINT ERROR_MESSAGE();
		THROW;
	END CATCH

END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Jing S. Zhong
-- Create date: 2023-02-19
-- Description:	cronjob_task_runner
-- =============================================
/*
EXEC [dbo].[cronjob_task_runner]
	@RUNNER_ID = 1;
*/
CREATE PROCEDURE [dbo].[cronjob_task_runner]
	@RUNNER_ID INT = 1
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @ARCHIEVE TABLE (
		[run_id] [int] NOT NULL,
		[run_tsk_id] [int] NOT NULL,
		[run_crn_id] [int] NOT NULL,
		[run_status] [nvarchar](50) NOT NULL,
		[run_this_run] [datetime] NOT NULL,
		[run_next_run] [datetime] NULL,
		[run_runner_id] [int] NULL
	);

	BEGIN TRY
		INSERT INTO @ARCHIEVE
		EXEC [dbo].[cronjob_task_picker]
			@RUNNER_ID = @RUNNER_ID,
			@STATUS = 'running';

		DECLARE @RUN_ID [int]
			,@DELIMETER [nvarchar](100)
			,@RUN_DETAIL [nvarchar](MAX);
		DECLARE	@TASK_ID [int]
			,@STEP_ID [int]
			,@STEP_NAME [nvarchar](250)
			,@STEP_TYPE [bit]
			,@STEP_FAILURE_CONTINUE [bit]
			,@STEP_SCRIPT [nvarchar](MAX);

		DECLARE TSAK_STEPS CURSOR FOR
		SELECT act.[stp_tsk_id]
			,act.[stp_step_id]
			,act.[stp_name]
			,act.[stp_type]
			,ISNULL(act.[stp_failure_continue],0) [stp_failure_continue]
			,act.[stp_script]
		FROM [dbo].[cronjob_step] act
		JOIN [dbo].[cronjob_running] run
		ON act.[stp_tsk_id] = run.[run_tsk_id]
		JOIN @ARCHIEVE ach
		ON run.[run_id] = ach.[run_id]
		ORDER BY act.[stp_tsk_id]
			,act.[stp_step_id];

		OPEN TSAK_STEPS;
		FETCH TSAK_STEPS INTO 
			@TASK_ID
			,@STEP_ID
			,@STEP_NAME
			,@STEP_TYPE
			,@STEP_FAILURE_CONTINUE
			,@STEP_SCRIPT;

		SELECT @DELIMETER = ''
			,@RUN_DETAIL = '';
		BEGIN TRY
			WHILE @@FETCH_STATUS = 0
			BEGIN
				--PRINT @STEP_SCRIPT;
				SET @RUN_DETAIL = @RUN_DETAIL + @DELIMETER + @STEP_NAME;
				BEGIN TRY
					EXEC (@STEP_SCRIPT);
				END TRY
				BEGIN CATCH
					SET @RUN_DETAIL = @RUN_DETAIL + @DELIMETER + ERROR_MESSAGE();
					IF @STEP_FAILURE_CONTINUE = 0 THROW;
				END CATCH

				SET @DELIMETER = NCHAR(13);
				FETCH TSAK_STEPS INTO 
					@TASK_ID
					,@STEP_ID
					,@STEP_NAME
					,@STEP_TYPE
					,@STEP_FAILURE_CONTINUE
					,@STEP_SCRIPT;
			END
		END TRY
		BEGIN CATCH
			SET @RUN_DETAIL = @RUN_DETAIL + @DELIMETER + ERROR_MESSAGE();
		END CATCH

		CLOSE TSAK_STEPS;
		DEALLOCATE TSAK_STEPS;

		SELECT @RUN_ID = [run_id]
		FROM @ARCHIEVE;
		INSERT INTO @ARCHIEVE
		EXEC [dbo].[cronjob_task_resolver]
			@RUNNER_ID = @RUNNER_ID,
			@STATUS = 'complete',
			@QUEUE_ID = @RUN_ID,
			@RUN_DETAIL = @RUN_DETAIL;
	END TRY
	BEGIN CATCH
		-- handle the failure with que and his
		PRINT '--- cronjob_task_runner failure ---'
		PRINT ERROR_MESSAGE();
		THROW;
	END CATCH

	SELECT * FROM @ARCHIEVE;
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Jing S. Zhong
-- Create date: 2023-02-19
-- Description:	cronjob_task_resolver
-- =============================================
/*
EXEC [dbo].[cronjob_task_resolver]
	@RUNNER_ID = 1,
	@STATUS = 'complete',
	@QUEUE_ID = NULL,
	@RUN_DETAIL = NULL;
*/
CREATE PROCEDURE [dbo].[cronjob_task_resolver]
	@RUNNER_ID INT = 1,
	@STATUS [nvarchar](50) = 'complete',
	@QUEUE_ID INT = NULL,
	@RUN_DETAIL [nvarchar](MAX) = NULL
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @ARCHIEVE TABLE (
		[run_id] [int] NOT NULL,
		[run_tsk_id] [int] NOT NULL,
		[run_crn_id] [int] NOT NULL,
		[run_status] [nvarchar](50) NOT NULL,
		[run_this_run] [datetime] NOT NULL,
		[run_next_run] [datetime] NULL,
		[run_runner_id] [int] NULL
	);

	BEGIN TRAN
	BEGIN TRY
		WITH [task_targeted] AS (
			SELECT TOP (1) [run_id]
					,[run_tsk_id]
					,[run_crn_id]
					,[run_status]
					,[run_this_run]
					,[run_next_run]
					,[run_runner_id]
			FROM [dbo].[cronjob_running]
			WHERE [run_runner_id] = @RUNNER_ID
				AND [run_id] = ISNULL(@QUEUE_ID, [run_id])
			ORDER BY [run_id]
			)
		DELETE [task_targeted]
		OUTPUT deleted.[run_id]
			,deleted.[run_tsk_id]
			,deleted.[run_crn_id]
			,@STATUS [run_status]
			,deleted.[run_this_run]
			,deleted.[run_next_run]
			,deleted.[run_runner_id]
		INTO @ARCHIEVE;

		UPDATE his
		SET [his_runner_id] = run.[run_runner_id]
			,[his_status] = run.[run_status]
			,[his_completed] = GETDATE()
			,[his_detail] = @RUN_DETAIL
		FROM [dbo].[cronjob_history] his
		JOIN @ARCHIEVE run
		ON his.[his_tsk_id] = run.[run_tsk_id]
		AND his.[his_crn_id] = run.[run_crn_id]
		AND his.[his_scheduled] = run.[run_this_run]
		COMMIT;

		SELECT * FROM @ARCHIEVE;

	END TRY
	BEGIN CATCH
		-- handle the failure with run and his
		ROLLBACK;
		PRINT '--- rollback ---'
		PRINT ERROR_MESSAGE();
		THROW;
	END CATCH

END
GO
