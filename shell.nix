# PyCharm is marked unfree in nixpkgs (the unified build ships the free
# Community tier), so allow it here instead of requiring a global
# allowUnfree. `jetbrains.pycharm-community` was removed upstream in 2025
# after JetBrains discontinued the separate Community edition.
{ pkgs ? import <nixpkgs> {
    config.allowUnfreePredicate = pkg:
      (pkg.pname or (builtins.parseDrvName (pkg.name or "")).name) == "pycharm";
  }
}:

pkgs.mkShell {
    buildInputs = [
      pkgs.git
      pkgs.python311
      pkgs.docker
      pkgs.jetbrains.pycharm
    ];
}
