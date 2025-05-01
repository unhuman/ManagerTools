package com.unhuman.managertools

@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.codehaus.gpars', module='gpars', version='1.2.1'),
])

import com.unhuman.flexidb.FlexiDB
import com.unhuman.flexidb.FlexiDBQueryColumn
import com.unhuman.flexidb.data.FlexiDBRow
import com.unhuman.managertools.data.DBData
import com.unhuman.managertools.data.DBIndexData
import com.unhuman.managertools.data.UserActivity
import com.unhuman.managertools.rest.SourceControlREST
import com.unhuman.managertools.util.CommandLineHelper
import groovy.cli.commons.CliBuilder
import groovyx.gpars.GParsPool
import groovyx.gpars.util.PoolUtils
import org.apache.commons.cli.OptionGroup

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

/**
 * This class analyzes individual contributions based on version control activities within specific time windows.
 * Unlike SprintReportIndividualAnalysis which focuses on sprint-based data, this class focuses on raw activity
 * based on timestamps in version control, independent of sprint structures.
 */
class IndividualContributionsAnalysis extends SprintReportTeamAnalysis {
    
    // Date formats for parsing user input
    private static final SimpleDateFormat USER_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    private static final String CONFIG_FILENAME = ".managerTools.cfg"
    
    // Time window properties
    private LocalDate startDate
    private LocalDate endDate
    private int periodDays = 14  // Default period is 2 weeks (14 days)
    private List<String> teamUsers
    private String bitbucketServer

    // GitHub Enterprise settings
    private List<String> repositories = new ArrayList<>()
    
    /**
     * Override the setupRun method to include repository specification
     */
    @Override
    protected void setupRun() {
        CliBuilder cli = new CliBuilder(usage: 'IndividualContributionsAnalysis [options]', header: 'Options:')
        cli.width = 120
        cli.h(longOpt: 'help', 'Shows useful information')
        
        def boardOrTeamGroup = new OptionGroup(required: true)
        boardOrTeamGroup.with {
            addOption(cli.option('b', [longOpt: 'boardId', args: 1, argName: 'boardId'], 'Sprint Board Id Number'))
            addOption(cli.option('t', [longOpt: 'teamName', args: 1, argName: 'team'], 'Sprint Team Name'))
        }
        cli.options.addOptionGroup(boardOrTeamGroup)
        
        cli.q(longOpt: 'quietMode', 'Quiet mode (use default/stored values without prompt)')
        cli.i(longOpt: 'isolateTicket', required: false, args:1, argName: 'isolateTicket',  'Isolate ticket for processing (for debugging)')
        cli.o(longOpt: 'outputCSV', required: true, args: 1, argName: 'outputCSV', 'Output filename (.csv)')
        cli.mt(longOpt: 'multithread', required: false, args: 1, argName: 'number', 'Number of threads, default to 1, *=cores')
        cli.includeMergeCommits(required: false, 'Include merge commit data in code metrics')
        cli.maxCommitSize(required: false, args: 1, argName: 'number', 'Limit the amount of size (adds+removes) of a commit to be counted in code metrics')
        
        // Add date-specific options
        cli.sd(longOpt: 'startDate', required: false, args: 1, argName: 'startDate', 'Start date for analysis (yyyy-MM-dd)')
        cli.ed(longOpt: 'endDate', required: false, args: 1, argName: 'endDate', 'End date for analysis (yyyy-MM-dd)')
        cli.p(longOpt: 'periodDays', required: false, args: 1, argName: 'periodDays', 'Length of each period in days (default: 14)')
        cli.n(longOpt: 'periods', required: false, args: 1, argName: 'periods', 'Number of periods to analyze')
        
        // Add repositories options
        cli.r(longOpt: 'repositories', required: false, args: 1, argName: 'repos', 'Comma-separated list of repositories to analyze')
        cli.repos(longOpt: 'repoList', required: false, args: 1, argName: 'repoList', 'File containing list of repositories (one per line)')
        
        // Parse command line - directly assign to the instance variable
        commandLineOptions = cli.parse(this.args)
        
        if (!commandLineOptions) {
            System.exit(-1)
        }
        
        // Validate output file
        if (!commandLineOptions.'outputCSV'.endsWith(".csv")) {
            throw new RuntimeException("Output filename must end in .csv")
        }
        
        if (commandLineOptions.h) {
            cli.usage()
            System.exit(0)
        }
        
        // Setup services (jira, github, bitbucket)
        setupServices()
        
        // Setup date parameters
        CommandLineHelper commandLineHelper = new CommandLineHelper(CONFIG_FILENAME)
        if (commandLineOptions.q) {
            commandLineHelper.setQuietModeNoPrompts()
        }
        
        setupDateParameters(commandLineHelper)
        
        // Get repositories to analyze - priority order:
        // 1. Command line -r/--repositories option
        // 2. Command line --repoList file option
        // 3. Config file
        // 4. Prompt the user
        if (commandLineOptions.r) {
            repositories = commandLineOptions.r.split(",").collect { it.trim() }
            System.out.println("Analyzing repositories from command line: ${repositories}")
        } else if (commandLineOptions.repos) {
            // Read from file
            String repoListFile = commandLineOptions.repos
            File repoFile = new File(repoListFile)
            
            if (repoFile.exists() && repoFile.canRead()) {
                repositories = repoFile.readLines().findAll { it && !it.trim().startsWith('#') }.collect { it.trim() }
                System.out.println("Analyzing ${repositories.size()} repositories from file: ${repoListFile}")
            } else {
                System.err.println("Warning: Repository list file ${repoListFile} not found or not readable")
            }
        }
        
        // If no repositories specified via command line, try config or prompt
        if (repositories.isEmpty()) {
            // Try to get from config
            try {
                String reposString = commandLineHelper.getConfigFileManager().getValue("repositories")
                if (reposString) {
                    repositories = reposString.split(",").collect { it.trim() }
                    System.out.println("Using repositories from config: ${repositories}")
                }
            } catch (Exception e) {
                // Config didn't have it, prompt user
                if (!commandLineOptions.q) {
                    // Replace getBasic() with getValue() or another appropriate method
                    System.out.println("Please enter repositories to analyze (comma-separated):")
                    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))
                    String reposString = reader.readLine()
                    
                    if (reposString && !reposString.trim().isEmpty()) {
                        repositories = reposString.split(",").collect { it.trim() }
                        // Store for future use
                        try {
                            commandLineHelper.getConfigFileManager().setValue("repositories", reposString)
                        } catch (Exception ex) {
                            // Ignore if we can't save to config
                        }
                        System.out.println("Analyzing repositories: ${repositories}")
                    } else {
                        System.out.println("No repositories specified, will attempt to discover based on user activity")
                    }
                } else {
                    System.out.println("No repositories specified and quiet mode enabled, will attempt to discover based on user activity")
                }
            }
        }
        
        // Get team users
        teamUsers = commandLineHelper.getTeamBoardUsers(teamName, boardId)
        System.out.println() // helps with output formatting
    }
    
    private void setupDateParameters(CommandLineHelper commandLineHelper) {
        // Get start and end dates from command line options or prompt user
        if (getCommandLineOptions().sd) {
            startDate = LocalDate.parse(getCommandLineOptions().sd, DATE_FORMATTER)
        } else {
            String startDateStr = commandLineHelper.getDateCheck("for start of analysis", "contributionStartDate")
            startDate = LocalDate.parse(startDateStr, DATE_FORMATTER)
        }
        
        if (getCommandLineOptions().ed) {
            endDate = LocalDate.parse(getCommandLineOptions().ed, DATE_FORMATTER)
        } else if (getCommandLineOptions().n && getCommandLineOptions().p) {
            // Calculate end date based on number of periods and period length
            int periods = Integer.parseInt(getCommandLineOptions().n)
            periodDays = Integer.parseInt(getCommandLineOptions().p)
            endDate = startDate.plusDays((long) periodDays * periods)
        } else if (getCommandLineOptions().n) {
            // Use default period length
            int periods = Integer.parseInt(getCommandLineOptions().n)
            endDate = startDate.plusDays((long) periodDays * periods)
        } else if (getCommandLineOptions().p) {
            // Use provided period length but ask for end date
            periodDays = Integer.parseInt(getCommandLineOptions().p)
            String endDateStr = commandLineHelper.getDateCheck("for end of analysis", "contributionEndDate")
            endDate = LocalDate.parse(endDateStr, DATE_FORMATTER)
        } else {
            // Ask for end date
            String endDateStr = commandLineHelper.getDateCheck("for end of analysis", "contributionEndDate")
            endDate = LocalDate.parse(endDateStr, DATE_FORMATTER)
        }
        
        if (endDate.isBefore(startDate)) {
            throw new RuntimeException("End date must be after start date")
        }
        
        // Update period length if we have specific dates but no period length specified
        if (!getCommandLineOptions().p) {
            long daysBetween = ChronoUnit.DAYS.between(startDate, endDate)
            int periods = getCommandLineOptions().n ? Integer.parseInt(getCommandLineOptions().n) : 1
            periodDays = (int) Math.ceil((double) daysBetween / periods)
        }
        
        System.out.println("Analysis period: ${startDate} to ${endDate} (${ChronoUnit.DAYS.between(startDate, endDate)} days)")
        System.out.println("Using period length of ${periodDays} days")
    }
    
    // We need to override the run method as well to use null for sprintIds and cycles
    @Override
    def run() {
        setupRun()

        long time = System.currentTimeMillis()

        aggregateData(teamName, boardId, Mode.KANBAN, null, null)
        generateOutput()

        time = (long) ((System.currentTimeMillis() - time) / 1000)
        Calendar.instance.with {
            clear()
            set(Calendar.SECOND, (Integer) time)
            System.out.println("Time to process: ${format('HH:mm:ss')}")
        }
    }
    
    @Override
    protected List<String> generateColumnsOrder() {
        List<String> columnOrder = super.generateColumnsOrder()
        
        // Add back the COMMENT specifics columns
        int indexAddBack = columnOrder.indexOf(UserActivity.COMMENTED.name())
        if (indexAddBack >= 0) {
            columnOrder.remove(indexAddBack)
            columnOrder.add(indexAddBack++, UserActivity.COMMENTED_ON_SELF.name())
            columnOrder.add(indexAddBack++, UserActivity.COMMENTED_ON_OTHERS.name())
            columnOrder.add(indexAddBack++, UserActivity.OTHERS_COMMENTED.name())
        }
        
        return columnOrder
    }
    
    @Override
    protected def aggregateData(String teamName, String boardId, Mode mode, List<String> sprintIds, Long cycles) {
        database = new FlexiDB(generateDBSignature(), true)
        
        // Specify threads
        Integer threadCount = getCommandLineOptions().mt
                ? (getCommandLineOptions().mt.equals("*")
                ? PoolUtils.retrieveDefaultPoolSize() : Integer.parseInt(getCommandLineOptions().mt))
                : 1
        System.out.println("Using ${threadCount} threads")
        
        // Calculate the periods to analyze
        List<Map<String, Object>> periods = calculatePeriods()
        
        // For each period, find activities in version control systems directly
        for (int periodIndex = 0; periodIndex < periods.size(); periodIndex++) {
            Map<String, Object> periodInfo = periods[periodIndex]
            String periodName = "Period ${periodIndex + 1}"
            LocalDate periodStart = (LocalDate) periodInfo.get("start")
            LocalDate periodEnd = (LocalDate) periodInfo.get("end")
            
            System.out.println("Processing ${periodName}: ${periodStart} to ${periodEnd}")
            
            // Convert LocalDate to Date for time comparisons
            Date periodStartTime = Date.from(periodStart.atStartOfDay(ZoneId.systemDefault()).toInstant())
            Date periodEndTime = Date.from(periodEnd.atTime(23, 59, 59).atZone(ZoneId.systemDefault()).toInstant())
            
            // Create period data structure for the database
            Map<String, Object> periodData = new HashMap<>()
            periodData.name = periodName
            periodData.startDate = DATE_TIME_PARSER.format(periodStartTime)
            periodData.endDate = DATE_TIME_PARSER.format(periodEndTime)
            
            // Query repositories directly rather than going through Jira
            getSourceControlContributions(threadCount, periodData, periodStartTime, periodEndTime)
        }
        
        return null
    }
    
    /**
     * Calculates time periods between startDate and endDate based on periodDays
     * @return List of period maps, each containing start and end dates
     */
    private List<Map<String, Object>> calculatePeriods() {
        List<Map<String, Object>> periods = new ArrayList<>()
        
        LocalDate currentStart = startDate
        while (currentStart.isBefore(endDate) || currentStart.isEqual(endDate)) {
            LocalDate currentEnd = currentStart.plusDays(periodDays - 1)
            if (currentEnd.isAfter(endDate)) {
                currentEnd = endDate
            }
            
            Map<String, Object> period = new HashMap<>()
            period.put("start", currentStart)
            period.put("end", currentEnd)
            periods.add(period)
            
            currentStart = currentEnd.plusDays(1)
            
            // Break if we've reached or passed the end date
            if (currentStart.isAfter(endDate)) {
                break
            }
        }
        
        return periods
    }
    
    /**
     * Gets contributions from source control systems directly without relying on Jira
     */
    private def getSourceControlContributions(int threadCount, Map<String, Object> period,
                                             Date periodStartTime, Date periodEndTime) {
        String periodName = period.name
        String startDate = cleanDate(period.startDate)
        String endDate = cleanDate(period.endDate)
        
        System.out.println("Fetching source control activities between ${startDate} and ${endDate}")
        
        try {
            // Instead of finding repositories, we'll process activities for all users directly
            // Check if users were specified
            if (teamUsers == null || teamUsers.isEmpty()) {
                System.out.println("No users specified for analysis")
                return
            }
            
            System.out.println("Analyzing source control activities for ${teamUsers.size()} users")
            
            // Process each user in parallel
            AtomicInteger userCount = new AtomicInteger(0)
            GParsPool.withPool(threadCount) {
                teamUsers.eachParallel(user -> {
                    System.out.println("Processing user ${userCount.incrementAndGet()}/${teamUsers.size()}: ${user}")
                    
                    // Process user activities from GitHub
                    processGitHubUserActivities(user, periodName, startDate, endDate, periodStartTime, periodEndTime)
                    
                    // Process user activities from BitBucket
                    processBitBucketUserActivities(user, periodName, startDate, endDate, periodStartTime, periodEndTime)
                })
            }
        } catch (Exception e) {
            System.err.println("Error accessing source control: ${e.message}")
            e.printStackTrace()
        }
    }
    
    /**
     * Process GitHub user activities
     */
    private void processGitHubUserActivities(String username, 
                                          String periodName, 
                                          String startDate, 
                                          String endDate, 
                                          Date periodStartTime, 
                                          Date periodEndTime) {
        try {
            System.out.println("   Fetching GitHub activities for user: ${username}")
            
            // In a full implementation, you would:
            // 1. Call GitHub API to get user's activity (commits, PRs, comments)
            // 2. Filter by date range
            // 3. Process each activity
            
            // For now, we'll directly gather user PRs, commits and activities
            List<Object> userActivities = getAllGitHubUserActivities(username, periodStartTime, periodEndTime)
            
            if (userActivities.isEmpty()) {
                System.out.println("   No GitHub activities found for ${username} in time period")
                return
            }
            
            System.out.println("   Found ${userActivities.size()} GitHub activities for ${username}")
            
            // Process each activity
            userActivities.each { activity ->
                processUserActivity(activity, "github", username, periodName, startDate, endDate, periodStartTime, periodEndTime)
            }
            
        } catch (Exception e) {
            System.err.println("Error processing GitHub activities for user ${username}: ${e.message}")
        }
    }
    
    /**
     * Process BitBucket user activities
     */
    private void processBitBucketUserActivities(String username, 
                                             String periodName, 
                                             String startDate, 
                                             String endDate, 
                                             Date periodStartTime, 
                                             Date periodEndTime) {
        try {
            System.out.println("   Fetching BitBucket activities for user: ${username}")
            
            // In a full implementation, you would:
            // 1. Call BitBucket API to get user's activity (commits, PRs, comments)
            // 2. Filter by date range
            // 3. Process each activity
            
            // For now, we'll directly gather user PRs, commits and activities
            List<Object> userActivities = getAllBitBucketUserActivities(username, periodStartTime, periodEndTime)
            
            if (userActivities.isEmpty()) {
                System.out.println("   No BitBucket activities found for ${username} in time period")
                return
            }
            
            System.out.println("   Found ${userActivities.size()} BitBucket activities for ${username}")
            
            // Process each activity
            userActivities.each { activity ->
                processUserActivity(activity, "bitbucket", username, periodName, startDate, endDate, periodStartTime, periodEndTime)
            }
            
        } catch (Exception e) {
            System.err.println("Error processing BitBucket activities for user ${username}: ${e.message}")
        }
    }
    
    /**
     * Get all GitHub activities for a user within a time period
     * Using the existing GithubREST client for all API operations
     */
    private List<Object> getAllGitHubUserActivities(String username, Date periodStartTime, Date periodEndTime) {
        List<Object> activities = new ArrayList<>()
        
        try {
            // Use the existing githubREST client which handles authentication and API details
            if (githubREST == null || githubREST instanceof com.unhuman.managertools.rest.NullREST) {
                System.out.println("   GitHub REST client not configured or no token available")
                return activities
            }
            
            System.out.println("   Using configured GitHub client for user: ${username}")
            
            // Check if we have specific repositories to analyze
            if (repositories == null || repositories.isEmpty()) {
                System.out.println("   No specific repositories specified, attempting to discover repositories")
                // Try to discover user repositories using the githubREST client
                try {
                    def userRepos = githubREST.getUserRepositories(username)
                    if (userRepos != null && userRepos.size() > 0) {
                        System.out.println("   Found ${userRepos.size()} repositories for user ${username}")
                        
                        // Process each discovered repository
                        for (repo in userRepos) {
                            String repoName = repo.name
                            processGitHubRepository(username, repoName, periodStartTime, periodEndTime, activities)
                        }
                    }
                } catch (Exception e) {
                    System.err.println("   Error discovering GitHub repositories for user ${username}: ${e.message}")
                }
            } else {
                System.out.println("   Analyzing ${repositories.size()} specified repositories")
                
                // Process each specified repository
                repositories.each { repo ->
                    processGitHubRepository(username, repo, periodStartTime, periodEndTime, activities)
                }
            }
        } catch (Exception e) {
            System.err.println("   Error fetching GitHub activities for user ${username}: ${e.message}")
            e.printStackTrace()
        }
        
        return activities
    }
    
    /**
     * Process a GitHub repository for a specific user's activities
     */
    private void processGitHubRepository(String username, String repo, Date periodStartTime, Date periodEndTime, List<Object> activities) {
        try {
            System.out.println("      Checking GitHub repository: ${repo}")
            
            // Get pull requests in this repository by this user within the time period
            List<Object> pullRequests = new ArrayList<>()
            try {
                // Use the REST client to get PRs by user/repo
                def prs = githubREST.getPullRequestsByUser(repo, username)
                if (prs != null) {
                    // Filter by date range
                    prs.each { pr ->
                        if (pr.createdAt >= periodStartTime.getTime() && pr.createdAt <= periodEndTime.getTime()) {
                            pullRequests.add(pr)
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("      Error getting pull requests for ${repo}: ${e.message}")
            }
            
            System.out.println("      Found ${pullRequests.size()} PRs by ${username} in ${repo} within time period")
            
            // Process each pull request
            pullRequests.each { pr ->
                Map<String, Object> prInfo = new HashMap<>()
                prInfo.put("type", "pull_request")
                prInfo.put("action", "OPENED")
                prInfo.put("user", username)
                prInfo.put("pr_id", pr.id)
                prInfo.put("repo", repo)
                prInfo.put("url", pr.url)
                prInfo.put("timestamp", pr.createdAt)
                activities.add(prInfo)
                
                // Also get associated commits
                try {
                    def commits = githubREST.getCommits(pr.url)
                    if (commits != null) {
                        System.out.println("      PR ${pr.id} has ${commits.values.size()} commits")
                        
                        // Add each commit as an activity
                        for (int i = 0; i < commits.values.size(); i++) {
                            def commit = (commits instanceof List) ? commits.get(i) : commits.values.get(i)
                            if (commit.committer.name.equalsIgnoreCase(username) || 
                                commit.author.name.equalsIgnoreCase(username)) {
                                
                                // Check if the commit is in our time range
                                Long commitTimestamp = commit.committerTimestamp
                                if (commitTimestamp >= periodStartTime.getTime() && 
                                    commitTimestamp <= periodEndTime.getTime()) {
                                    
                                    Map<String, Object> commitActivity = new HashMap<>()
                                    commitActivity.put("type", "commit")
                                    commitActivity.put("user", username)
                                    commitActivity.put("repo", repo)
                                    commitActivity.put("sha", commit.id)
                                    commitActivity.put("message", commit.message)
                                    commitActivity.put("url", pr.url)  // Use PR URL as context
                                    commitActivity.put("timestamp", commitTimestamp)
                                    activities.add(commitActivity)
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("      Error getting commits for PR ${pr.id}: ${e.message}")
                }
                
                // Get PR comments
                try {
                    def prActivities = githubREST.getActivities(pr.url)
                    if (prActivities != null) {
                        // Add comments as activities
                        for (int i = 0; i < prActivities.values.size(); i++) {
                            def prActivity = (prActivities instanceof List) ? prActivities[i] : prActivities.values.get(i)
                            if (prActivity.action == "COMMENTED" && 
                                prActivity.user.name.equalsIgnoreCase(username)) {
                                
                                // Check if the comment is in our time range
                                if (prActivity.createdDate >= periodStartTime.getTime() && 
                                    prActivity.createdDate <= periodEndTime.getTime()) {
                                    
                                    Map<String, Object> commentActivity = new HashMap<>()
                                    commentActivity.put("type", "comment")
                                    commentActivity.put("user", username)
                                    commentActivity.put("repo", repo)
                                    commentActivity.put("pr_id", pr.id)
                                    commentActivity.put("text", prActivity.comment)
                                    commentActivity.put("url", pr.url)
                                    commentActivity.put("timestamp", prActivity.createdDate)
                                    commentActivity.put("pr_author", pr.author)
                                    activities.add(commentActivity)
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("      Error getting comments for PR ${pr.id}: ${e.message}")
                }
            }
            
            // Get direct commits to the repository (not associated with PRs)
            try {
                def commits = githubREST.getRepositoryCommits(repo, username, periodStartTime, periodEndTime)
                if (commits != null) {
                    System.out.println("      Found ${commits.size()} direct commits by ${username} in ${repo}")
                    
                    // Add each commit as an activity
                    commits.each { commit ->
                        Map<String, Object> commitActivity = new HashMap<>()
                        commitActivity.put("type", "commit")
                        commitActivity.put("user", username)
                        commitActivity.put("repo", repo)
                        commitActivity.put("sha", commit.id)
                        commitActivity.put("message", commit.message)
                        commitActivity.put("url", commit.url)
                        commitActivity.put("timestamp", commit.committerTimestamp)
                        
                        // Try to get commit diff stats if available
                        try {
                            def diffStats = githubREST.getCommitDiffs(commit.url, commit.id)
                            if (diffStats != null) {
                                commitActivity.put("additions", diffStats.additions)
                                commitActivity.put("deletions", diffStats.deletions)
                            }
                        } catch (Exception e) {
                            // Continue without diff stats if not available
                        }
                        
                        activities.add(commitActivity)
                    }
                }
            } catch (Exception e) {
                System.err.println("      Error getting direct commits for ${repo}: ${e.message}")
            }
            
        } catch (Exception e) {
            System.err.println("      Error processing repository ${repo}: ${e.message}")
        }
    }
    
    /**
     * Get all BitBucket activities for a user within a time period
     * Using the existing BitbucketREST client for all API operations
     */
    private List<Object> getAllBitBucketUserActivities(String username, Date periodStartTime, Date periodEndTime) {
        List<Object> activities = new ArrayList<>()
        
        try {
            // Use the existing bitbucketREST client which handles authentication and API details
            if (bitbucketREST == null || bitbucketREST instanceof com.unhuman.managertools.rest.NullREST) {
                System.out.println("   BitBucket REST client not configured or no credentials available")
                return activities
            }
            
            System.out.println("   Using configured BitBucket client for user: ${username}")
            
            // Check if we have specific repositories to analyze
            if (repositories == null || repositories.isEmpty()) {
                System.out.println("   No specific repositories specified, attempting to discover repositories")
                // Try to discover user repositories using the bitbucketREST client
                try {
                    def userRepos = bitbucketREST.getUserRepositories(username)
                    if (userRepos != null && userRepos.size() > 0) {
                        System.out.println("   Found ${userRepos.size()} repositories for user ${username}")
                        
                        // Process each discovered repository
                        for (repo in userRepos) {
                            String repoName = repo.name
                            processBitBucketRepository(username, repoName, periodStartTime, periodEndTime, activities)
                        }
                    }
                } catch (Exception e) {
                    System.err.println("   Error discovering BitBucket repositories for user ${username}: ${e.message}")
                }
            } else {
                System.out.println("   Analyzing ${repositories.size()} specified repositories")
                
                // Process each specified repository
                repositories.each { repo ->
                    processBitBucketRepository(username, repo, periodStartTime, periodEndTime, activities)
                }
            }
        } catch (Exception e) {
            System.err.println("   Error fetching BitBucket activities for user ${username}: ${e.message}")
            e.printStackTrace()
        }
        
        return activities
    }
    
    /**
     * Process a BitBucket repository for a specific user's activities
     */
    private void processBitBucketRepository(String username, String repo, Date periodStartTime, Date periodEndTime, List<Object> activities) {
        try {
            System.out.println("      Checking BitBucket repository: ${repo}")
            
            // Get pull requests in this repository by this user within the time period
            List<Object> pullRequests = new ArrayList<>()
            try {
                // Use the REST client to get PRs by user/repo
                def prs = bitbucketREST.getPullRequestsByUser(repo, username)
                if (prs != null) {
                    // Filter by date range
                    prs.each { pr ->
                        if (pr.createdAt >= periodStartTime.getTime() && pr.createdAt <= periodEndTime.getTime()) {
                            pullRequests.add(pr)
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("      Error getting pull requests for ${repo}: ${e.message}")
            }
            
            System.out.println("      Found ${pullRequests.size()} PRs by ${username} in ${repo} within time period")
            
            // Process each pull request
            pullRequests.each { pr ->
                Map<String, Object> prInfo = new HashMap<>()
                prInfo.put("type", "pull_request")
                prInfo.put("action", "OPENED")
                prInfo.put("user", username)
                prInfo.put("pr_id", pr.id)
                prInfo.put("repo", repo)
                prInfo.put("url", pr.url)
                prInfo.put("timestamp", pr.createdAt)
                activities.add(prInfo)
                
                // Also get associated commits
                try {
                    def commits = bitbucketREST.getCommits(pr.url)
                    if (commits != null) {
                        System.out.println("      PR ${pr.id} has ${commits.values.size()} commits")
                        
                        // Add each commit as an activity
                        for (int i = 0; i < commits.values.size(); i++) {
                            def commit = (commits instanceof List) ? commits.get(i) : commits.values.get(i)
                            if (commit.committer.name.equalsIgnoreCase(username) || 
                                commit.author.name.equalsIgnoreCase(username)) {
                                
                                // Check if the commit is in our time range
                                Long commitTimestamp = commit.committerTimestamp
                                if (commitTimestamp >= periodStartTime.getTime() && 
                                    commitTimestamp <= periodEndTime.getTime()) {
                                    
                                    Map<String, Object> commitActivity = new HashMap<>()
                                    commitActivity.put("type", "commit")
                                    commitActivity.put("user", username)
                                    commitActivity.put("repo", repo)
                                    commitActivity.put("sha", commit.id)
                                    commitActivity.put("message", commit.message)
                                    commitActivity.put("url", pr.url)  // Use PR URL as context
                                    commitActivity.put("timestamp", commitTimestamp)
                                    activities.add(commitActivity)
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("      Error getting commits for PR ${pr.id}: ${e.message}")
                }
                
                // Get PR comments
                try {
                    def prActivities = bitbucketREST.getActivities(pr.url)
                    if (prActivities != null) {
                        // Add comments as activities
                        for (int i = 0; i < prActivities.values.size(); i++) {
                            def prActivity = (prActivities instanceof List) ? prActivities[i] : prActivities.values.get(i)
                            if (prActivity.action == "COMMENTED" && 
                                prActivity.user.name.equalsIgnoreCase(username)) {
                                
                                // Check if the comment is in our time range
                                if (prActivity.createdDate >= periodStartTime.getTime() && 
                                    prActivity.createdDate <= periodEndTime.getTime()) {
                                    
                                    Map<String, Object> commentActivity = new HashMap<>()
                                    commentActivity.put("type", "comment")
                                    commentActivity.put("user", username)
                                    commentActivity.put("repo", repo)
                                    commentActivity.put("pr_id", pr.id)
                                    commentActivity.put("text", prActivity.comment)
                                    commentActivity.put("url", pr.url)
                                    commentActivity.put("timestamp", prActivity.createdDate)
                                    commentActivity.put("pr_author", pr.author)
                                    activities.add(commentActivity)
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("      Error getting comments for PR ${pr.id}: ${e.message}")
                }
            }
            
            // Get direct commits to the repository (not associated with PRs)
            try {
                def commits = bitbucketREST.getRepositoryCommits(repo, username, periodStartTime, periodEndTime)
                if (commits != null) {
                    System.out.println("      Found ${commits.size()} direct commits by ${username} in ${repo}")
                    
                    // Add each commit as an activity
                    commits.each { commit ->
                        Map<String, Object> commitActivity = new HashMap<>()
                        commitActivity.put("type", "commit")
                        commitActivity.put("user", username)
                        commitActivity.put("repo", repo)
                        commitActivity.put("sha", commit.id)
                        commitActivity.put("message", commit.message)
                        commitActivity.put("url", commit.url)
                        commitActivity.put("timestamp", commit.committerTimestamp)
                        
                        // Try to get commit diff stats if available
                        try {
                            def diffStats = bitbucketREST.getCommitDiffs(commit.url, commit.id)
                            if (diffStats != null) {
                                commitActivity.put("additions", diffStats.additions)
                                commitActivity.put("deletions", diffStats.deletions)
                            }
                        } catch (Exception e) {
                            // Continue without diff stats if not available
                        }
                        
                        activities.add(commitActivity)
                    }
                }
            } catch (Exception e) {
                System.err.println("      Error getting direct commits for ${repo}: ${e.message}")
            }
            
        } catch (Exception e) {
            System.err.println("      Error processing repository ${repo}: ${e.message}")
        }
    }
    
    /**
     * Process a single user activity
     */
    private void processUserActivity(Map<String, Object> activity,
                                   String sourceType,
                                   String username,
                                   String periodName,
                                   String startDate,
                                   String endDate,
                                   Date periodStartTime,
                                   Date periodEndTime) {
        try {
            // Get activity details
            String activityType = activity.get("type")
            String action = activity.get("action")
            String repo = activity.get("repo")
            String prId = activity.get("pr_id")
            String url = activity.get("url")
            Long timestamp = activity.get("timestamp")
            
            // Check if activity is in the time range
            if (timestamp < periodStartTime.getTime() || timestamp >= periodEndTime.getTime()) {
                return
            }
            
            System.out.println("      Processing ${activityType} ${action} by ${username} in ${repo}")
            
            // Create a "ticket" placeholder for DB indexing
            String ticket = repo + "-" + prId
            
            // Create lookup to store activity in database
            List<FlexiDBQueryColumn> indexLookup = createIndexLookup(periodName, ticket, prId, username)
            
            // Store baseline information
            populateBaselineDBInfo(indexLookup, startDate, endDate, username)
            
            // Process based on activity type
            switch (activityType) {
                case "pull_request":
                    processPullRequestActivity(activity, sourceType, username, indexLookup, periodStartTime, periodEndTime)
                    break
                case "commit":
                    processCommitActivity(activity, sourceType, username, indexLookup)
                    break
                case "comment":
                    processCommentActivity(activity, sourceType, username, indexLookup)
                    break
                default:
                    // Unknown activity type
                    break
            }
            
        } catch (Exception e) {
            System.err.println("Error processing activity: ${e.message}")
        }
    }
    
    /**
     * Process a pull request activity
     */
    private void processPullRequestActivity(Map<String, Object> activity,
                                          String sourceType,
                                          String username,
                                          List<FlexiDBQueryColumn> indexLookup,
                                          Date periodStartTime,
                                          Date periodEndTime) {
        try {
            // Determine the PR action type
            String action = activity.get("action")
            UserActivity prActivityAction = UserActivity.getResolvedValue(action)
            
            if (prActivityAction == null) {
                System.err.println("      Unknown PR action: ${action}")
                return
            }
            
            // Handle GitHub action conversions
            if (prActivityAction.toString() == "DISMISSED") {
                prActivityAction = UserActivity.DECLINED
            }
            
            // Record activity count
            incrementCounter(indexLookup, prActivityAction)
            
            // If URL is provided, try to get additional information
            String url = activity.get("url")
            if (url != null && !url.isEmpty()) {
                // Use the existing source control REST clients
                SourceControlREST sourceControlREST = "github".equals(sourceType) ? githubREST : bitbucketREST
                
                // Convert URL to API URL
                String apiUrl = sourceControlREST.apiConvert(url)
                
                // Try to get PR diffs
                def diffsResponse = sourceControlREST.getDiffs(apiUrl)
                if (diffsResponse != null) {
                    processDiffs(PR_PREFIX, diffsResponse, indexLookup)
                }
                
                // Try to get PR commits
                def commits = sourceControlREST.getCommits(apiUrl)
                if (commits != null) {
                    // Process each commit in the PR
                    for (int i = commits.values.size() - 1; i >= 0; i--) {
                        def commit = (commits instanceof List) ? commits.get(i) : commits.values.get(i)
                        String commitSHA = commit.id
                        Long commitTimestamp = commit.committerTimestamp
                        
                        // Check if commit is within our time period
                        if (periodStartTime.getTime() > commitTimestamp || commitTimestamp >= periodEndTime.getTime()) {
                            continue
                        }
                        
                        // Skip merge commits if configured to do so
                        if (!getCommandLineOptions().includeMergeCommits && commit.message.matches(MERGE_COMMIT_REGEX)) {
                            continue
                        }
                        
                        // Process commit diff
                        String commitUrl = (commit.url != null) ? commit.url : apiUrl
                        def commitDiffs = sourceControlREST.getCommitDiffs(commitUrl, commitSHA)
                        if (commitDiffs != null) {
                            processDiffs(COMMIT_PREFIX, commitDiffs, indexLookup)
                        }
                        
                        // Record commit message
                        def commitMessage = commit.message.replaceAll("(\\r|\\n)?\\n", "  ").trim()
                        database.append(indexLookup, DBData.COMMIT_MESSAGES.name(), commitMessage, true)
                    }
                }
                
                // Try to get PR activities (comments, etc.)
                def prActivities = sourceControlREST.getActivities(apiUrl)
                if (prActivities != null) {
                    def commentBlockers = new ArrayList<CommentBlocker>()
                    
                    System.out.println("      PR has ${prActivities.values.size()} activities")
                    
                    // Process activities
                    for (int i = prActivities.values.size() - 1; i >= 0; i--) {
                        def prActivity = (prActivities instanceof List) ? prActivities[i] : prActivities.values.get(i)
                        
                        // Check if activity is within time period
                        if (prActivity.createdDate < periodStartTime.getTime() || 
                            prActivity.createdDate >= periodEndTime.getTime()) {
                            continue
                        }
                        
                        // Process comments specially
                        if (prActivity.action == UserActivity.COMMENTED.name()) {
                            processComment(commentBlockers, indexLookup, username, prActivity)
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error processing PR activity: ${e.message}")
        }
    }
    
    /**
     * Process a commit activity
     */
    private void processCommitActivity(Map<String, Object> activity,
                                    String sourceType,
                                    String username,
                                    List<FlexiDBQueryColumn> indexLookup) {
        try {
            // Record commit message if available
            String message = activity.get("message")
            if (message != null) {
                message = message.replaceAll("(\\r|\\n)?\\n", "  ").trim()
                database.append(indexLookup, DBData.COMMIT_MESSAGES.name(), message, true)
            }
            
            // Record commit stats if available
            Integer additions = activity.get("additions")
            Integer deletions = activity.get("deletions")
            
            if (additions != null || deletions != null) {
                // Check if we should ignore large commits
                if (getCommandLineOptions().maxCommitSize &&
                        (additions + deletions >= Integer.parseInt(getCommandLineOptions().maxCommitSize))) {
                    return
                }
                
                incrementCounter(indexLookup, UserActivity.valueOf(COMMIT_PREFIX + "ADDED"), additions != null ? additions : 0)
                incrementCounter(indexLookup, UserActivity.valueOf(COMMIT_PREFIX + "REMOVED"), deletions != null ? deletions : 0)
            }
            
        } catch (Exception e) {
            System.err.println("Error processing commit activity: ${e.message}")
        }
    }
    
    /**
     * Process a comment activity
     */
    private void processCommentActivity(Map<String, Object> activity,
                                     String sourceType,
                                     String username,
                                     List<FlexiDBQueryColumn> indexLookup) {
        try {
            // Record comment text if available
            String commentText = activity.get("text")
            if (commentText != null) {
                commentText = commentText.replaceAll("(\\r|\\n)?\\n", "  ").trim()
                database.append(indexLookup, DBData.COMMENTS.name(), commentText, true)
                
                // Increment comment counter
                incrementCounter(indexLookup, UserActivity.COMMENTED)
                
                // Check if comment is on own PR or others
                String prAuthor = activity.get("pr_author")
                if (prAuthor != null) {
                    if (prAuthor.equalsIgnoreCase(username)) {
                        incrementCounter(indexLookup, UserActivity.COMMENTED_ON_SELF)
                    } else {
                        incrementCounter(indexLookup, UserActivity.COMMENTED_ON_OTHERS)
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error processing comment activity: ${e.message}")
        }
    }
    
    @Override
    protected void setupServices() {
        super.setupServices()
        
        // Initialize bitbucketServer variable from the BitBucketREST client
        CommandLineHelper commandLineHelper = new CommandLineHelper(CONFIG_FILENAME)
        bitbucketServer = commandLineHelper.getBitbucketServer()
        
        System.out.println("Using BitBucket server: ${bitbucketServer}")
    }
    
    @Override
    protected void generateOutput() {
        // Determine the list of columns to report
        List<String> columnOrder = generateColumnsOrder()
        
        // Get list of periods and users
        List<String> periods = database.findUniqueValues(DBIndexData.SPRINT.name())
        Set<String> authors = new LinkedHashSet<>(database.findUniqueValues(DBData.AUTHOR.name()))
        Set<String> users = new LinkedHashSet<>(database.findUniqueValues(DBIndexData.USER.name()))
        
        // Adjust teamUsers list based on input (* for authors, ** for all users)
        if (teamUsers.size() == 1) {
            if (teamUsers.get(0) == "*") { // authors
                teamUsers = new ArrayList<>(authors)
            } else if (teamUsers.get(0) == "**") { // all users
                teamUsers = new ArrayList<>(users)
            }
        }
        
        // Try to case-insensitively match user names
        List<String> specifiedUsers = (teamUsers.size() > 0) ? teamUsers : new ArrayList<>(users)
        users = new LinkedHashSet<>()
        for (String specifiedUser : specifiedUsers) {
            boolean matched = false
            for (String existingUser : database.findUniqueValues(DBIndexData.USER.name())) {
                if (existingUser.equalsIgnoreCase(specifiedUser)) {
                    users.add(existingUser)
                    matched = true
                    break
                }
            }
            if (!matched) {
                users.add(specifiedUser)
            }
        }
        
        // For each user, generate an individual report
        for (String user : users) {
            StringBuilder sb = new StringBuilder(4096)
            sb.append(FlexiDBRow.headingsToCSV(columnOrder))
            sb.append('\n')
            
            FlexiDBRow overallTotalsRow = new FlexiDBRow(columnOrder.size())
            
            // Process data for each period
            for (String period : periods) {
                List<FlexiDBQueryColumn> userPeriodFinder = new ArrayList<>()
                userPeriodFinder.add(new FlexiDBQueryColumn(DBIndexData.SPRINT.name(), period))
                userPeriodFinder.add(new FlexiDBQueryColumn(DBIndexData.USER.name(), user))
                
                findRowsAndAppendCSVData(userPeriodFinder, sb, overallTotalsRow)
            }
            
            // Add summary
            appendSummary(sb, overallTotalsRow)
            
            // Write to file
            String dataIndicator = (teamName != null) ? teamName : boardId
            String filename = getCommandLineOptions().'outputCSV'.replace(".csv", "-${dataIndicator}-${user}.csv")
            writeResultsFile(filename, sb)
        }
        
        // Generate a team summary report as well
        generateTeamSummary(periods, columnOrder, users);
    }
    
    private void generateTeamSummary(List<String> periods, List<String> columnOrder, Set<String> users) {
        StringBuilder sb = new StringBuilder(4096)
        sb.append(FlexiDBRow.headingsToCSV(columnOrder))
        sb.append('\n')
        
        FlexiDBRow overallTotalsRow = new FlexiDBRow(columnOrder.size())
        
        // For each period
        for (String period : periods) {
            List<FlexiDBQueryColumn> periodFinder = new ArrayList<>()
            periodFinder.add(new FlexiDBQueryColumn(DBIndexData.SPRINT.name(), period))
            
            findRowsAndAppendCSVData(periodFinder, sb, overallTotalsRow)
        }
        
        // Add summary
        appendSummary(sb, overallTotalsRow)
        
        // Write to file
        String dataIndicator = (teamName != null) ? teamName : boardId
        String filename = getCommandLineOptions().'outputCSV'.replace(".csv", "-${dataIndicator}.csv")
        writeResultsFile(filename, sb)
    }
}