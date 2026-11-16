const path = require('path');

module.exports = {
  preset: '@react-native/jest-preset',
  // monet-rn lives outside this app (symlinked, like the Metro
  // nodeModulesPaths setup): its modules resolve deps from here.
  moduleDirectories: ['node_modules', path.resolve(__dirname, 'node_modules')],
};
