{ pkgs ? import <nixpkgs> { } }:

pkgs.mkShell {
  buildInputs = [
    pkgs.parallel
    pkgs.python39
    pkgs.python39Packages.black
    pkgs.python39Packages.mypy
    pkgs.python39Packages.isort
    pkgs.strace
    pkgs.time
  ];
}
