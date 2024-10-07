def rotate(arr,shift)
  n=arr.length
  shift = shift % n 
  store_elements = []
  (n - shift...n).each { |i| store_elements << arr[i] }
  (0...n - shift).each { |i| store_elements << arr[i] }
  store_elements
end
array = [1,2,3,4,5]
shift = 2
result = rotate(array,shift)
puts "Result is : #{result}"
