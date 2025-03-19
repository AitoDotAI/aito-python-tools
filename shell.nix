{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
    buildInputs = [
      pkgs.git
      pkgs.python311
      pkgs.docker
      pkgs.jetbrains.pycharm-community
    ];
}
