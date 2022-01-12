import json
import random
import pprint

transit_map = {
    (1, 2): 10,
    (1, 3): 7,
    (2, 8): 40,
    (2, 4): 5,
    (3, 4): 8,
    (3, 5): 20,
    (4, 6): 17,
    (4, 7): 30,
    (5, 7): 25,
    (6, 8): 15,
    (6, 7): 15,
    (7, 9): 12,
    (8, 9): 22
}

entity_routes = [
    [1, 2, 8, 9],
    [1, 2, 4, 6, 8, 9],
    [1, 2, 4, 6, 7, 9],
    [1, 2, 4, 7, 9],
    [1, 3, 4, 6, 8, 9],
    [1, 3, 4, 6, 7, 9],
    [1, 3, 4, 7, 9],
    [1, 3, 5, 7, 9]
]

def add_reverse_map(transit_map):
    added_map = {k: v for k, v in transit_map.items()}
    for (origin, dest), transit_time in transit_map.items():
        added_map[(dest, origin)] = transit_time
    return added_map

def add_reverse_routes(routes):
    added_routes = [i for i in routes]
    for route in routes:
        added_routes.append(list(reversed(route)))
    return added_routes

transit_map = add_reverse_map(transit_map)
entity_routes = add_reverse_routes(entity_routes)

if __name__ == '__main__':
    print('Transit map:')
    pprint.pprint(transit_map)
    print('\nEntity routes:')
    pprint.pprint(entity_routes)
    print()
    entity_count = 100

    entity_data = []

    # Generate data with noise
    for entity_id in range(1, entity_count+1):
        planned_route = random.choice(entity_routes)
        print(f'(entity_id = {entity_id}): planned_route: {planned_route}')

        for node_index in range(len(planned_route)-1):
            origin = planned_route[node_index]
            dest = planned_route[node_index+1]
            average_transit_time = transit_map[(origin, dest)]
            std_deviation = 5
            actual_transit_time = random.gauss(average_transit_time, std_deviation)
            if actual_transit_time < 0:
                actual_transit_time = 3

            data_point = {'entity_id': entity_id, 'origin_id': origin, 'destination_id': dest, 'travel_time': actual_transit_time}
            entity_data.append(data_point)

    file_path = 'trip_data.json'
    with open(file_path, 'w') as f:
        f.write('[\n')
        for record in entity_data:
            f.write(json.dumps(record) + ',\n')
        f.write(']')

    #pprint.pprint(entity_data)
    print(f'Output to {file_path}')


