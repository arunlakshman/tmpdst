# Optional bootstrap convenience. Metadata returns only the answering broker
# (decisions 29/33) and a client sticks to the broker it bootstrapped to, so the
# NLB spreads CONNECTIONS, not load. Internal, one subnet, no cross-zone, no SG
# (targets are instances, client IP is preserved, health checks come from the
# subnet CIDR which broker SG allows when use_nlb).
resource "aws_lb" "kafka" {
  count = var.use_nlb ? 1 : 0

  name                             = "${local.name}-kafka"
  load_balancer_type               = "network"
  internal                         = true
  subnets                          = [aws_subnet.public.id]
  enable_cross_zone_load_balancing = false
  tags                             = { Name = "${local.name}-kafka" }
}

resource "aws_lb_target_group" "kafka" {
  count = var.use_nlb ? 1 : 0

  name                 = "${local.name}-kafka"
  port                 = 9092
  protocol             = "TCP"
  target_type          = "instance"
  vpc_id               = aws_vpc.this.id
  deregistration_delay = 30
  tags                 = { Name = "${local.name}-kafka" }

  health_check {
    protocol            = "TCP"
    port                = "9092"
    interval            = 10
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }
}

resource "aws_lb_target_group_attachment" "kafka" {
  count            = var.use_nlb ? var.broker_count : 0
  target_group_arn = aws_lb_target_group.kafka[0].arn
  target_id        = aws_instance.broker[count.index].id
  port             = 9092
}

resource "aws_lb_listener" "kafka" {
  count             = var.use_nlb ? 1 : 0
  load_balancer_arn = aws_lb.kafka[0].arn
  port              = 9092
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.kafka[0].arn
  }
}
