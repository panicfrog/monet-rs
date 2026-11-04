plugins {
    id("com.android.application")
    // AGP 9 builds Kotlin sources natively; the separate Kotlin plugin is
    // gone (and rejected) since 9.0.
}

android {
    namespace = "com.monet.demo"
    compileSdk = 36

    defaultConfig {
        applicationId = "com.monet.demo"
        minSdk = 24
        targetSdk = 36
        versionCode = 1
        versionName = "0.1"
        ndk {
            abiFilters += "arm64-v8a"
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

// Builds the monet-demo Rust library into jniLibs via cargo-ndk. The
// battery inside it is the same code the host integration test runs.
val cargoBuild by tasks.registering(Exec::class) {
    workingDir = File("../../..") // the workspace root
    commandLine(
        "cargo", "ndk",
        "--target", "arm64-v8a",
        "-o", file("src/main/jniLibs").absolutePath,
        "build", "-p", "monet-demo",
    )
}

tasks.matching { it.name.startsWith("preBuild") }.configureEach {
    dependsOn(cargoBuild)
}

dependencies {
    // The JVM component of rustls-platform-verifier (matches the native
    // crate via the rustls-platform-verifier-android version-sync crate).
    implementation(files("libs/rustls-platform-verifier-0.2.0.aar"))
}
