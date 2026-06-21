import { Alert, Button, Group, JsonInput, Modal, Stack, Text, TextInput } from "@mantine/core";
import { useForm } from "@mantine/form";
import { notifications } from "@mantine/notifications";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";

import { ApiError, Workload, api } from "@/api/client";

interface Props {
  opened: boolean;
  onClose: () => void;
  workload: Workload | null;
}

interface FormValues {
  pk: string;
  extra: string; // JSON
}

export default function TaskEnqueueModal({ opened, onClose, workload }: Props) {
  const { t } = useTranslation();
  const qc = useQueryClient();

  const form = useForm<FormValues>({
    initialValues: { pk: "", extra: "{}" },
    validate: {
      pk: (v) => (v.trim().length > 0 ? null : "required"),
      extra: (v) => {
        try {
          const o = JSON.parse(v);
          return o && typeof o === "object" && !Array.isArray(o)
            ? null
            : t("workloads.enqueue.invalid_extra");
        } catch {
          return t("workloads.create.invalid_json");
        }
      },
    },
  });

  useEffect(() => {
    if (opened) {
      form.reset();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [opened, workload?.slug]);

  const mut = useMutation({
    mutationFn: ({ slug, pk, extra }: { slug: string; pk: string; extra: Record<string, unknown> }) =>
      api.enqueueTask(slug, pk, extra),
    onSuccess: (res, vars) => {
      const dup = res.duplicates > 0;
      notifications.show({
        color: dup ? "yellow" : "green",
        title: dup
          ? t("workloads.enqueue.duplicate", { pk: vars.pk })
          : t("workloads.enqueue.queued", { pk: vars.pk }),
        message: "",
      });
      qc.invalidateQueries({ queryKey: ["queue", vars.slug] });
      qc.invalidateQueries({ queryKey: ["runs", vars.slug] });
      onClose();
    },
    onError: (e: Error) => {
      const body = e instanceof ApiError ? JSON.stringify(e.body) : e.message;
      notifications.show({
        color: "red",
        title: t("workloads.enqueue.failed"),
        message: body,
      });
    },
  });

  const submit = (v: FormValues) => {
    if (!workload) return;
    mut.mutate({ slug: workload.slug, pk: v.pk.trim(), extra: JSON.parse(v.extra) });
  };

  if (!workload) return null;

  return (
    <Modal
      opened={opened}
      onClose={onClose}
      title={
        <Text fw={600}>
          {t("workloads.enqueue.title", { slug: workload.slug })}
        </Text>
      }
      size="md"
      centered
    >
      <form onSubmit={form.onSubmit(submit)}>
        <Stack>
          <Alert variant="light" color="blue">
            {t("workloads.enqueue.help")}
          </Alert>
          <TextInput
            label={t("workloads.enqueue.pk")}
            description={t("workloads.enqueue.pk_help")}
            placeholder="任意の文字列 (例: 1, abc, http://...)"
            required
            {...form.getInputProps("pk")}
          />
          <JsonInput
            label={t("workloads.enqueue.extra")}
            description={t("workloads.enqueue.extra_help")}
            autosize
            minRows={3}
            formatOnBlur
            validationError={t("workloads.create.invalid_json")}
            {...form.getInputProps("extra")}
          />
          <Group justify="flex-end" mt="sm">
            <Button variant="default" onClick={onClose} disabled={mut.isPending}>
              {t("workloads.create.cancel")}
            </Button>
            <Button type="submit" loading={mut.isPending}>
              {t("workloads.enqueue.submit")}
            </Button>
          </Group>
        </Stack>
      </form>
    </Modal>
  );
}
