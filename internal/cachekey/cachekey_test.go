package cachekey

import "testing"

func TestKeyIgnoresKeyOrderAndWhitespace(t *testing.T) {
	a, err := Key("mock", []byte(`{"x":1,"y":[1,2]}`), []byte(`{"prompt":"cube","seed":3}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Key("mock", []byte("{ \"y\": [1, 2], \"x\": 1 }"), []byte(`{"seed":3, "prompt":"cube"}`))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("keys differ: %s vs %s", a, b)
	}
}

func TestKeySeparatesFields(t *testing.T) {
	base, _ := Key("mock", []byte(`{}`), []byte(`{"seed":1}`))
	cases := map[string][3]string{
		"job type": {"exec", `{}`, `{"seed":1}`},
		"config":   {"mock", `{"model":"m"}`, `{"seed":1}`},
		"payload":  {"mock", `{}`, `{"seed":2}`},
		"number":   {"mock", `{}`, `{"seed":1.0}`},
	}
	for name, c := range cases {
		k, err := Key(c[0], []byte(c[1]), []byte(c[2]))
		if err != nil {
			t.Fatal(err)
		}
		if k == base {
			t.Errorf("%s change did not change the key", name)
		}
	}
}

func TestCanonicalRejectsBadJSON(t *testing.T) {
	if _, err := Canonical([]byte(`{"a":`)); err == nil {
		t.Fatal("expected error")
	}
	if _, err := Canonical([]byte(`{} {}`)); err == nil {
		t.Fatal("expected error for trailing data")
	}
}
