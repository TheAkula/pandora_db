package pandora_db

type AssertError struct {}

func (err AssertError) Error() string {
	return "assert"
}

func assert(condition bool) {	
	if !condition {
		err := AssertError{}		
		panic(err)
	}
}