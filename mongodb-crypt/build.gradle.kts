/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import ProjectExtensions.configureJarManifest
import ProjectExtensions.configureMavenPublication
import de.undercouch.gradle.tasks.download.Download

plugins {
    id("project.java")
    alias(libs.plugins.download)
}

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(libs.jna)
}

configureMavenPublication {
    pom {
        name.set("MongoCrypt")
        description.set("MongoDB client-side crypto support")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "com.mongodb.crypt.capi"
    attributes["Bundle-Name"] = "MongoCrypt"
    attributes["Bundle-SymbolicName"] = "com.mongodb.crypt.capi"
    attributes["Import-Package"] = "org.slf4j.*;resolution:=optional,org.bson.*"
    attributes["-exportcontents"] = "com.mongodb.*;-noimport:=true"
    attributes["Private-Package"] = ""
}

/*
 * Jna copy or download resources
 */
val jnaDownloadsDir = rootProject.file("build/jnaLibs/downloads/").path
val jnaResourcesDir = rootProject.file("build/jnaLibs/resources/").path
val jnaLibPlatform: String =
    if (com.sun.jna.Platform.RESOURCE_PREFIX.startsWith("darwin")) "darwin" else com.sun.jna.Platform.RESOURCE_PREFIX
val jnaLibsPath: String = System.getProperty("jnaLibsPath", "${jnaResourcesDir}${jnaLibPlatform}")
val jnaResources: String = System.getProperty("jna.library.path", jnaLibsPath)

// Download jnaLibs that match the git tag or revision to jnaResourcesBuildDir
val downloadRevision = "1.14.1"
val binariesArchiveName = "libmongocrypt-java.tar.gz"

/**
 * The name of the archive includes downloadRevision to ensure that:
 * - the archive is downloaded if the revision changes.
 * - the archive is not downloaded if the revision is the same and archive had already been saved in build output.
 */
val localBinariesArchiveName = "libmongocrypt-java-$downloadRevision.tar.gz"

val downloadUrl: String =
    "https://mciuploads.s3.amazonaws.com/libmongocrypt/java/$downloadRevision/$binariesArchiveName"

val jnaMapping: Map<String, String> =
    mapOf(
        "rhel-62-64-bit" to "linux-x86-64",
        "rhel72-zseries-test" to "linux-s390x",
        "rhel-71-ppc64el" to "linux-ppc64le",
        "ubuntu1604-arm64" to "linux-aarch64",
        "windows-test" to "win32-x86-64",
        "macos" to "darwin")

sourceSets { main { java { resources { srcDirs(jnaResourcesDir) } } } }

tasks.register<Download>("downloadJava") {
    src(downloadUrl)
    dest("${jnaDownloadsDir}/$localBinariesArchiveName")
    overwrite(true)
    /* To make sure we don't download archive with binaries if it hasn't been changed in S3 bucket since last download.*/
    onlyIfModified(true)
}

tasks.register<Copy>("unzipJava") {
    /*
       Clean up the directory first if the task is not UP-TO-DATE.
       This can happen if the download revision has been changed and the archive is downloaded again.
    */
    doFirst {
        println("Cleaning up $jnaResourcesDir")
        delete(jnaResourcesDir)
    }
    from(tarTree(resources.gzip("${jnaDownloadsDir}/$localBinariesArchiveName")))
    include(
        jnaMapping.keys.flatMap {
            listOf(
                "${it}/nocrypto/**/libmongocrypt.so", "${it}/lib/**/libmongocrypt.dylib", "${it}/bin/**/mongocrypt.dll")
        })
    eachFile { path = "${jnaMapping[path.substringBefore("/")]}/${name}" }
    into(jnaResourcesDir)
    dependsOn("downloadJava")

    doLast { println("jna.library.path contents: \n  ${fileTree(jnaResourcesDir).files.joinToString(",\n  ")}") }
}

// The `processResources` task (defined by the `java-library` plug-in) consumes files in the main
// source set.
// Add a dependency on `unzipJava`. `unzipJava` adds libmongocrypt libraries to the main source set.
tasks.processResources { mustRunAfter(tasks.named("unzipJava")) }

tasks.register("downloadJnaLibs") { dependsOn("downloadJava", "unzipJava") }

tasks.test {
    systemProperty("jna.debug_load", "true")
    systemProperty("jna.library.path", jnaResources)
    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }

    doFirst {
        println("jna.library.path contents:")
        println(fileTree(jnaResources) { this.setIncludes(listOf("*.*")) }.files.joinToString(",\n  ", "  "))
    }
    dependsOn("downloadJnaLibs", "downloadJava", "unzipJava")
}

tasks.withType<AbstractPublishToMaven> {
    description =
        """$description
        | System properties:
        | =================
        |
        | jnaLibsPath    : Custom local JNA library path for inclusion into the build (rather than downloading from s3)
        | gitRevision    : Optional Git Revision to download the built resources for from s3.
    """.trimMargin()
}

tasks.withType<Jar> {
    // NOTE this enables depending on the mongocrypt from driver-core
    dependsOn("downloadJnaLibs")
}
