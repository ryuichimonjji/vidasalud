package cl.duoc.vidasalud.kafka.admin.controller;

import cl.duoc.vidasalud.kafka.admin.dto.GroupInfo;
import cl.duoc.vidasalud.kafka.admin.dto.TopicInfo;
import cl.duoc.vidasalud.kafka.admin.dto.TopicRequest;
import cl.duoc.vidasalud.kafka.admin.dto.TopicUpdateRequest;
import cl.duoc.vidasalud.kafka.admin.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, String>> create(@Valid @RequestBody TopicRequest req) {
        service.createTopic(req);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("mensaje", "Topico creado: " + req.getName()));
    }

    @GetMapping("/topics")
    public List<String> list() {
        return service.listTopics();
    }

    @GetMapping("/topics/{name}")
    public TopicInfo describe(@PathVariable String name) {
        return service.describeTopic(name);
    }

    @PutMapping("/topics/{name}")
    public Map<String, String> update(@PathVariable String name,
                                      @Valid @RequestBody TopicUpdateRequest req) {
        service.updateTopic(name, req);
        return Map.of("mensaje", "Topico actualizado: " + name);
    }

    @DeleteMapping("/topics/{name}")
    public Map<String, String> delete(@PathVariable String name) {
        service.deleteTopic(name);
        return Map.of("mensaje", "Topico eliminado: " + name);
    }

    @GetMapping("/cluster")
    public Map<String, Object> cluster() {
        return service.describeCluster();
    }

    @GetMapping("/groups")
    public List<String> groups() {
        return service.listGroups();
    }

    @GetMapping("/groups/{groupId}")
    public GroupInfo group(@PathVariable String groupId) {
        return service.describeGroup(groupId);
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> handle(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
    }
}