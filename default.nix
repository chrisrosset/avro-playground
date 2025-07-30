{ pkgs ? import <nixpkgs> {}}:

pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    avro-tools
    python312
    python312Packages.avro
    python312Packages.fastavro
  ];

  shellHook = ''
  '';
}
