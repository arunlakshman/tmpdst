package core

// ControlFirst is the InboxPolicy of decision G/H: control always wins, data
// only when control is empty. Stateless, so one value serves any number of nodes.
type ControlFirst struct{}

// Next pops from control if it is non-empty, otherwise from data.
func (ControlFirst) Next(control, data Queue) (EventData, bool) {
	if control.Len() > 0 {
		if b, ok := control.Pop(); ok {
			return b, true
		}
	}
	if data.Len() > 0 {
		return data.Pop()
	}
	return nil, false
}

var _ InboxPolicy = ControlFirst{}
