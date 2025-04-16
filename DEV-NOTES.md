# Developer notes (work in progress)

## Proposed Distribution Strategy:

Let's distribute based on functional stages and potential concurrency/resource pressure points:

1. **Resource Group 1:** `RG-AlmaAnalytics-CoreInfra`
   * **Purpose:** Holds shared, stateful resources.
   * **Azure Resources:**
     * Azure Storage Account(s) (for Blob Storage and Queue Storage used by all functions). You might even split Queues and Blobs into separate accounts if throughput becomes extreme, but start with one.
     * Azure SQL Database (or alternative) for Configuration & Job Tracking.
     * Azure Key Vault for secrets.
     * Application Insights resource (can be shared by all Function Apps for unified monitoring).
2. **Resource Group 2:** `RG-AlmaAnalytics-SchedulingIngest`
   * **Purpose:** Handles triggering cycles and the potentially high-concurrency, short-lived API fetch/pagination tasks.
   * **Azure Resources:**
     * **Function App 1:** `func-alma-scheduler`
       * **Plan:** Consumption Plan (Implicit)
       * **Functions:** All Timer Trigger functions (Schedulers).
       * **Reasoning:** Schedulers are typically low-frequency but initiate workflows. Keeping them separate is clean.
     * **Function App 2:** `func-alma-apifetcher`
       * **Plan:** Consumption Plan (Implicit)
       * **Functions:**
         * API Fetcher Function (triggered by `ApiFetchQueue`)
         * API Paginator Logic (likely part of the same function, re-triggering itself via the queue)
         * **Reasoning:** This is potentially the *highest concurrency* part. Fetching multiple reports, each paginating rapidly, can spawn many short-lived instances. Isolating this allows it to scale independently without impacting other stages' instance limits. It also isolates potential SNAT port exhaustion if many simultaneous calls are made to Alma.
3. **Resource Group 3:** `RG-AlmaAnalytics-Processing`
   * **Purpose:** Handles the core data manipulation which might be more CPU or memory-intensive per invocation, but perhaps less concurrently numerous than fetching.
   * **Azure Resources:**
     * **Function App 3:** `func-alma-dataprep`
       * **Plan:** Consumption Plan (Implicit)
       * **Functions:**
         * Data Combiner Function (triggered by `DataCombineQueue`)
         * Cross-Reference Function(s) (triggered by e.g., `CrossReferenceQueue`)
         * Update Preparation Function(s) (triggered by e.g., `UpdatePrepQueue`)
       * Reasoning: Groups the data transformation steps. Combining large files or complex cross-referencing might use more memory/CPU per function instance. Separating this allows it its own pool of resources and scaling limits.
4. **Resource Group 4:** `RG-AlmaAnalytics-Egress`
   * **Purpose:** Handles final steps involving external interactions (Alma updates, Email).
   * **Azure Resources:**
     * **Function App 4:** `func-alma-updater`
       * **Plan:** Consumption Plan (Implicit)
       * **Functions:**
         * Alma Updater Function (triggered by `AlmaUpdateQueue`)
       * **Reasoning:** Alma updates might involve rate limits or specific error handling. Isolating this keeps the core processing pipeline clean. Concurrency depends on how many records need updates across active jobs.
     * **Function App 5:** `func-alma-notifier`
       * **Plan:** Consumption Plan (Implicit)
       * **Functions:**
         * Report/Email Function (triggered by `EmailQueue` / `ReportQueue`)
         * Cleanup Function (optional, triggered by `CleanupQueue` or `Timer`)
       * **Reasoning:** Notifications and cleanup are generally lower-load final steps.

### Key Considerations:

* **Region:** Ensure ALL Resource Groups and resources (Storage, DB, Function Apps) are created in the **same Azure region** to minimize latency and avoid cross-region data transfer costs. Consumption Plan limits are often per-region.
* **Function App Granularity:** The breakdown above provides significant separation. You could potentially combine some (e.g., put Updating and Notifying in the same App if load isn't expected to be high) or separate further if specific processing steps prove uniquely resource-intensive. Start with logical separation like above and monitor.
* **Communication:** Functions in different apps communicate seamlessly via the shared Azure Queues and Blob Storage located in the `CoreInfra` resource group.
* **Deployment:** You will deploy different parts of your Python codebase to different Function Apps. Structure your project accordingly (e.g., separate folders or projects for code destined for each Function App). Consider using shared libraries for common code (like Alma API interaction logic).
* **Monitoring:** Ensure Application Insights is configured to track dependencies across all these Function Apps to get a full end-to-end view of your workflows. Using the same App Insights instance makes this easier.
* **Cost:** While you're spreading the load across multiple implicit Consumption plans, the billing model remains pay-per-execution and GB-seconds consumed across *all* functions. This architecture doesn't inherently increase cost vs. running in one plan *unless* it enables higher overall throughput that wasn't possible before due to limits. It's primarily about overcoming scaling/concurrency limits per plan.

This multi-Function App, multi-Resource Group structure gives you independent scaling contexts for different parts of your workflow, significantly reducing the risk of hitting Consumption Plan limits for the system as a whole, while also providing good organizational separation.
