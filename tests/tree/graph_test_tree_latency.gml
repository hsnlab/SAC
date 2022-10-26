graph [
  directed 1
  name "graph_test_tree_latency"
  node [
    id 0
    label "P"
  ]
  node [
    id 1
    label "1"
    time 60
    mem 2
  ]
  node [
    id 2
    label "2"
    time 28
    mem 3
  ]
  node [
    id 3
    label "3"
    time 17
    mem 2
  ]
  node [
    id 4
    label "4"
    time 33
    mem 3
  ]
  node [
    id 5
    label "5"
    time 75
    mem 3
  ]
  node [
    id 6
    label "6"
    time 27
    mem 1
  ]
  node [
    id 7
    label "7"
    time 58
    mem 3
  ]
  node [
    id 8
    label "8"
    time 57
    mem 1
  ]
  node [
    id 9
    label "9"
    time 94
    mem 3
  ]
  node [
    id 10
    label "10"
    time 53
    mem 2
  ]
  edge [
    source 0
    target 1
    rate 3
  ]
  edge [
    source 1
    target 2
    rate 3
  ]
  edge [
    source 1
    target 3
    rate 3
  ]
  edge [
    source 1
    target 4
    rate 2
  ]
  edge [
    source 2
    target 5
    rate 2
  ]
  edge [
    source 3
    target 6
    rate 1
  ]
  edge [
    source 3
    target 7
    rate 3
  ]
  edge [
    source 3
    target 8
    rate 2
  ]
  edge [
    source 4
    target 9
    rate 3
  ]
  edge [
    source 8
    target 10
    rate 1
  ]
]
