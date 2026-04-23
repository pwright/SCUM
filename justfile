set shell := ["bash", "-cu"]

init:
    python3 sysmvp.py init

demo:
    rm -rf examples/demo
    mkdir -p examples/demo
    printf 'hello\n' > examples/demo/a.txt
    printf 'world\n' > examples/demo/b.txt
    python3 sysmvp.py init
    python3 sysmvp.py scan --root examples/demo
    python3 sysmvp.py list

list:
    python3 sysmvp.py list

serve:
    python3 sysbrowse.py

history id:
    python3 sysmvp.py history {{id}}

test:
    tests/smoke.sh
