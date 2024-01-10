{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [ gcc ];
  # Needed for X11
  shellHook =
    let x11-libs = with pkgs; lib.makeLibraryPath [
        libGL
        xorg.libX11
        xorg.libXcursor
        xorg.libXi
        xorg.libXrandr
    ]; in
    ''export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:${x11-libs}"'';
}
