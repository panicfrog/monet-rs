pluginManagement {
    repositories {
        // CN mirrors first (a global init script covers project-level
        // repositories but not plugin resolution).
        maven("https://maven.aliyun.com/repository/google")
        maven("https://maven.aliyun.com/repository/central")
        maven("https://maven.aliyun.com/repository/gradle-plugin")
        maven("https://maven.aliyun.com/repository/public")
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    // No FAIL_ON_PROJECT_REPOS: a developer-global init script may inject
    // mirror repositories at project level.
    repositories {
        maven("https://maven.aliyun.com/repository/google")
        maven("https://maven.aliyun.com/repository/central")
        maven("https://maven.aliyun.com/repository/public")
        google()
        mavenCentral()
    }
}

rootProject.name = "monet-android-demo"
include(":app")
