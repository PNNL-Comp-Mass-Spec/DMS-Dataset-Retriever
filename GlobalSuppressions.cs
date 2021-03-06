﻿// This file is used by Code Analysis to maintain SuppressMessage
// attributes that are applied to this project.
// Project-level suppressions either have no target or are given
// a specific target and scoped to a namespace, type, member, etc.

using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:DMSDatasetRetriever.DMSDatasetRetriever.GetDatasetFileHashInfo(PRISMDatabaseUtils.IDBTools,System.Collections.Generic.IReadOnlyCollection{DMSDatasetRetriever.DatasetInfo})~System.Boolean")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:DMSDatasetRetriever.DMSDatasetRetriever.GetDatasetFolderPathInfo(PRISMDatabaseUtils.IDBTools,System.Collections.Generic.IEnumerable{DMSDatasetRetriever.DatasetInfo})~System.Boolean")]
[assembly: SuppressMessage("Usage", "RCS1246:Use element access.", Justification = "Prefer to use .First()", Scope = "member", Target = "~M:DMSDatasetRetriever.FileHashUtility.CreateChecksumFiles(System.Collections.Generic.IEnumerable{DMSDatasetRetriever.DatasetInfo},System.String)~System.Boolean")]
