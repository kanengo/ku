package mathx

import (
	"fmt"
	"testing"
)

func TestNextPowerOfTwo(t *testing.T) {
	type args struct {
		x uint64
	}
	tests := []struct {
		name string
		args args
		want uint64
	}{
		// TODO: Add test cases.
		{
			name: "test",
			args: args{
				x: 0,
			},
			want: 1,
		},
		{
			name: "test",
			args: args{
				x: 1,
			},
			want: 1,
		},
		{
			name: "test",
			args: args{
				x: 2,
			},
			want: 2,
		},
		{
			name: "test",
			args: args{
				x: 3,
			},
			want: 4,
		},
		{
			name: "test",
			args: args{
				x: 4,
			},
			want: 4,
		},
		{
			name: "test",
			args: args{
				x: 5,
			},
			want: 8,
		},
		{
			name: "test",
			args: args{
				x: 6,
			},
			want: 8,
		},
		{
			name: "test",
			args: args{
				x: 7,
			},
			want: 8,
		},
		{
			name: "test",
			args: args{
				x: 8,
			},
			want: 8,
		},
		{
			name: "test",
			args: args{
				x: 9,
			},
			want: 16,
		},
		{
			name: "test",
			args: args{
				x: 10,
			},
			want: 16,
		},
		{
			name: "test",
			args: args{
				x: 11,
			},
			want: 16,
		},
		{
			name: "test",
			args: args{
				x: 12,
			},
			want: 16,
		},
		{
			name: "test",
			args: args{
				x: 13,
			},
			want: 16,
		},
		{
			name: "test",
			args: args{
				x: 14,
			},
			want: 16,
		},
		{
			name: "test",
			args: args{
				x: 15,
			},
			want: 16,
		},
		{
			name: "test",
			args: args{
				x: 16,
			},
			want: 16,
		},
		{
			name: "test",
			args: args{
				x: 17,
			},
			want: 32,
		},
		{
			name: "test",
			args: args{
				x: 520,
			},
			want: 1024,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NextPowerOfTwo(tt.args.x); got != tt.want {
				fmt.Println(tt.args.x, got, tt.want)
				t.Errorf("NextPowerOfTwo() = %v, want %v", got, tt.want)
			}
		})
	}
}
