/**
 * RN autolinking config for monet-rn. The Android library (the
 * HttpEngine bridge + bootstrap) is discovered by the standard rule
 * (android/build.gradle present); the only addition is the package
 * instance, so the bridge bootstrap runs at app startup without any
 * app-side code.
 */
module.exports = {
  dependency: {
    platforms: {
      android: {
        packageInstance: 'new MonetRnPackage()',
      },
    },
  },
};
