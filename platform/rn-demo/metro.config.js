const path = require('path');
const { getDefaultConfig, mergeConfig } = require('@react-native/metro-config');

/**
 * Metro configuration
 * https://reactnative.dev/docs/metro
 *
 * @type {import('@react-native/metro-config').MetroConfig}
 */
const config = {
  // monet-rn is installed via npm `file:` as a symlink whose target sits
  // outside this project root; Metro only resolves/watches paths under
  // projectRoot + watchFolders, so the real package dir must be listed here.
  watchFolders: [path.resolve(__dirname, '../../packages/monet-rn')],
  resolver: {
    // Monet.ts's own `require('react-native')` resolves from the package's
    // real path, outside this project; without this list Metro's restricted
    // resolution cannot find the app's node_modules from there.
    nodeModulesPaths: [path.resolve(__dirname, 'node_modules')],
  },
};

module.exports = mergeConfig(getDefaultConfig(__dirname), config);
